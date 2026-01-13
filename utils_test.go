package gocbcoreps

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"github.com/golang/snappy"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

// TODO - should I have a GatewayOpsTestSuite
var TestOpts TestOptions

type TestOptions struct {
	Username       string
	Password       string
	BucketName     string
	ScopeName      string
	CollectionName string
	CngConnstr     string
	SrvConnstr     string
	ClientConn     *grpc.ClientConn
}

func envFlagString(envName, name, value, usage string) *string {
	envValue := os.Getenv(envName)
	if envValue != "" {
		value = envValue
	}
	return flag.String(name, value, usage)
}

var connStr = envFlagString("PSCONNSTR", "connstr", "",
	"Connection string for cng")
var srvConnStr = envFlagString("PSSRVCONNSTR", "srvconnstr", "",
	"Connection string for couchbase server")
var user = envFlagString("PSUSER", "user", "Administrator",
	"The username to use to authenticate when using a real server")
var password = envFlagString("PSPASS", "pass", "password",
	"The password to use to authenticate when using a real server")
var bucketName = envFlagString("PSBUCKET", "bucket", "default",
	"The bucket to use to test against")
var scopeName = envFlagString("PSSCOPE", "scope", "_default",
	"The scope to test against")
var collectionName = envFlagString("PSCOLLECTION", "collection", "_default",
	"The collection to test against")

func TestMain(m *testing.M) {
	SetupTests(m)
}

func SetupTests(m *testing.M) {
	flag.Parse()

	if *connStr == "" {
		panic("No connection string provided")
	}

	TestOpts.Username = *user

	TestOpts.Password = *password

	TestOpts.BucketName = *bucketName

	TestOpts.ScopeName = *scopeName

	TestOpts.CollectionName = *collectionName

	TestOpts.CngConnstr = *connStr

	TestOpts.SrvConnstr = *srvConnStr

	auth := NewBasicAuthenticator(*user, *password)
	conn, err := grpc.NewClient(*connStr,
		grpc.WithPerRPCCredentials(auth),
		grpc.WithTransportCredentials(credentials.NewTLS(
			// TODO - support testing with tls enabled
			&tls.Config{
				InsecureSkipVerify: true,
				//RootCAs:              pool,
				//GetClientCertificate: getClientCertificate,
			},
		)))
	if err != nil {
		panic(fmt.Sprintf("failed to create grpc client: %v", err))
	}

	TestOpts.ClientConn = conn
	result := m.Run()

	// TODO - do we need to add back leak checking?

	os.Exit(result)
}

func RandomDocId() string {
	return "test-doc" + "_" + uuid.NewString()
}

func requireRpcStatus(t *testing.T, err error, expectedCode codes.Code) {
	if expectedCode == codes.OK && err == nil {
		return
	}

	if err == nil {
		t.Fatalf("expected rpc error status but got nil")
	}

	errSt, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected rpc error status, but got non-status error")
	}

	if errSt.Code() != expectedCode {
		t.Logf("expected rpc error status code %s, but got %s: %s", expectedCode, errSt.Code(), errSt.Message())
	}
	require.Equal(t, expectedCode, errSt.Code())
}

func requireRpcErrorDetails[T any](t *testing.T, err error, checkFn func(detail *T)) {
	if err == nil {
		t.Fatalf("expected rpc error status but got nil")
	}

	errSt, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected rpc error status, but got non-status error")
	}

	requireStatusDetails(t, errSt, checkFn)
}

func requireStatusDetails[T any](t *testing.T, st *status.Status, checkFn func(detail *T)) {
	var expectedDetail *T
	var foundDetails []string
	for _, detail := range st.Details() {
		foundDetails = append(foundDetails, fmt.Sprintf("%T", detail))

		if reflect.TypeOf(detail) == reflect.TypeOf(expectedDetail) {
			checkFn(detail.(*T))
			return
		}
	}

	t.Fatalf("expected status detail of %T, but instead found [%s]",
		expectedDetail, strings.Join(foundDetails, ", "))
}

func createTestDoc(t *testing.T) string {
	kvClient := kv_v1.NewKvServiceClient(TestOpts.ClientConn)

	key := RandomDocId()
	_, err := kvClient.Insert(context.Background(), &kv_v1.InsertRequest{
		BucketName: TestOpts.BucketName,
		Key:        key,
		Content: &kv_v1.InsertRequest_ContentUncompressed{
			ContentUncompressed: TEST_CONTENT,
		},
		ContentFlags: TEST_CONTENT_FLAGS,
	})
	requireRpcStatus(t, err, codes.OK)

	return key
}

func createBinaryDoc(t *testing.T, content []byte) string {
	kvClient := kv_v1.NewKvServiceClient(TestOpts.ClientConn)

	key := RandomDocId()
	_, err := kvClient.Insert(context.Background(), &kv_v1.InsertRequest{
		BucketName: TestOpts.BucketName,
		Key:        key,
		Content: &kv_v1.InsertRequest_ContentUncompressed{
			ContentUncompressed: content,
		},
	})
	requireRpcStatus(t, err, codes.OK)

	return key
}

func createLockedDoc(t *testing.T) (string, uint64) {
	key := createTestDoc(t)
	cas := lockDoc(t, key)
	return key, cas
}

func lockDoc(t *testing.T, docId string) uint64 {
	kvClient := kv_v1.NewKvServiceClient(TestOpts.ClientConn)
	galResp, err := kvClient.GetAndLock(context.Background(), &kv_v1.GetAndLockRequest{
		BucketName:     TestOpts.BucketName,
		ScopeName:      TestOpts.ScopeName,
		CollectionName: TestOpts.CollectionName,
		Key:            docId,
		LockTime:       30,
	})
	requireRpcStatus(t, err, codes.OK)
	require.NotZero(t, galResp.Cas)
	return galResp.Cas
}

func decompressContent(t *testing.T, in []byte) []byte {
	if in == nil {
		return nil
	}

	out := make([]byte, len(in))
	out, err := snappy.Decode(out, in)
	require.NoError(t, err)
	return out
}

func requireValidMutationToken(t *testing.T, token *kv_v1.MutationToken, bucketName string) {
	require.NotNil(t, token)
	require.NotEmpty(t, token.BucketName)
	if bucketName != "" {
		require.Equal(t, bucketName, token.BucketName)
	}
	require.NotZero(t, token.VbucketUuid)
	require.NotZero(t, token.SeqNo)
}

type checkDocumentOptions struct {
	BucketName     string
	ScopeName      string
	CollectionName string
	DocId          string
	Cas            uint64
	Content        []byte
	ContentFlags   uint32
	CheckAsJson    bool

	expiry       expiryCheckType
	expiryBounds expiryCheckTypeWithinBounds
}

type expiryCheckType int

const (
	expiryCheckType_None expiryCheckType = iota
	expiryCheckType_Future
)

type expiryCheckTypeWithinBounds struct {
	MinSecs int
	MaxSecs int
}

func checkDocument(t *testing.T, opts checkDocumentOptions) {
	kvClient := kv_v1.NewKvServiceClient(TestOpts.ClientConn)
	getResp, err := kvClient.Get(context.Background(), &kv_v1.GetRequest{
		BucketName:     opts.BucketName,
		ScopeName:      opts.ScopeName,
		CollectionName: opts.CollectionName,
		Key:            opts.DocId,
	})

	// nil content means we expect the document not to exist...
	if opts.Content == nil {
		requireRpcStatus(t, err, codes.NotFound)
		return
	}

	// otherwise we assume it exists and apply the rules
	requireRpcStatus(t, err, codes.OK)
	require.NotZero(t, getResp.Cas)

	if !opts.CheckAsJson {
		assert.Equal(t, opts.Content, getResp.GetContentUncompressed())
	} else {
		assert.JSONEq(t, string(opts.Content), string(getResp.GetContentUncompressed()))
	}
	assert.Equal(t, opts.ContentFlags, getResp.ContentFlags)

	if opts.Cas != 0 {
		assert.Equal(t, opts.Cas, getResp.Cas)
	}

	switch opts.expiry {
	case expiryCheckType_None:
		assert.Nil(t, getResp.Expiry)
	case expiryCheckType_Future:
		require.NotNil(t, getResp.Expiry)
		ts := getResp.Expiry.AsTime()
		assert.True(t, !ts.Before(time.Now()))
	default:
		t.Fatalf("invalid test check, unknown expiry check type")
	}
}

func assertStatusProto(t *testing.T, st *spb.Status, expectedCode codes.Code) {
	assertStatus(t, status.FromProto(st), expectedCode)
}

func assertStatusProtoDetails[T any](t *testing.T, st *spb.Status, checkFn func(detail *T)) {
	assertStatusDetails(t, status.FromProto(st), checkFn)
}

func assertStatus(t *testing.T, st *status.Status, expectedCode codes.Code) {
	if expectedCode == codes.OK && st == nil {
		return
	}

	if st == nil {
		t.Fatalf("expected rpc error status but got nil")
	}

	assert.Equal(t, expectedCode, st.Code())
}

func assertStatusDetails[T any](t *testing.T, st *status.Status, checkFn func(detail *T)) {
	var expectedDetail *T
	var foundDetails []string
	for _, detail := range st.Details() {
		foundDetails = append(foundDetails, fmt.Sprintf("%T", detail))

		if reflect.TypeOf(detail) == reflect.TypeOf(expectedDetail) {
			checkFn(detail.(*T))
			return
		}
	}

	t.Fatalf("expected status detail of %T, but instead found [%s]",
		expectedDetail, strings.Join(foundDetails, ", "))
}

type ClusterInfoResponse struct {
	ImplementationVersion string `json:"implementationVersion"`
}

func getServerVersion(t *testing.T) string {
	ep := "http://" + TestOpts.SrvConnstr + ":8091"

	req, err := http.NewRequest("GET", ep+"/pools", nil)
	require.NoError(t, err)

	req.SetBasicAuth(TestOpts.Username, TestOpts.Password)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to get server version: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("failed to get server version: %v", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}

	var jsonResp ClusterInfoResponse
	err = json.Unmarshal(body, &jsonResp)
	require.NoError(t, err)

	// strip the meta-info like -enterprise or build numbers
	serverVersion := strings.Split(jsonResp.ImplementationVersion, "-")[0]

	return serverVersion
}

func IsOlderServerVersion(t *testing.T, checkVersion string) bool {
	serverVersion := getServerVersion(t)
	srvNodeVersion, err := newNodeVersion(serverVersion, false)
	require.NoError(t, err)

	checkNodeVersion, err := newNodeVersion(checkVersion, false)
	require.NoError(t, err)

	return srvNodeVersion.Lower(*checkNodeVersion)
}

func initialiseBucketAndScope(t *testing.T) (*string, *string) {
	var bucket, scope *string
	if !IsOlderServerVersion(t, "7.5.0") {
		bucket = &TestOpts.BucketName
		scope = &TestOpts.ScopeName
	}

	return bucket, scope
}

type NodeVersion struct {
	Major    int
	Minor    int
	Patch    int
	Build    int
	Edition  NodeEdition
	Modifier string
	IsMock   bool
}

type NodeEdition int

const (
	CommunityNodeEdition    = NodeEdition(1)
	EnterpriseNodeEdition   = NodeEdition(2)
	ProtostellarNodeEdition = NodeEdition(3)
)

func (v NodeVersion) Equal(ov NodeVersion) bool {
	if v.Major == ov.Major && v.Minor == ov.Minor &&
		v.Patch == ov.Patch && v.Edition == ov.Edition && v.Modifier == ov.Modifier {
		return true
	}
	return false
}

func (v NodeVersion) Higher(ov NodeVersion) bool {
	if v.Major > ov.Major {
		return true
	} else if v.Major < ov.Major {
		return false
	}

	if v.Minor > ov.Minor {
		return true
	} else if v.Minor < ov.Minor {
		return false
	}

	if v.Patch > ov.Patch {
		return true
	} else if v.Patch < ov.Patch {
		return false
	}

	if v.Build > ov.Build {
		return true
	} else if v.Build < ov.Build {
		return false
	}

	if v.Edition > ov.Edition {
		return true
	}

	return false
}

func (v NodeVersion) Lower(ov NodeVersion) bool {
	return !v.Higher(ov) && !v.Equal(ov)
}

func newNodeVersion(version string, isMock bool) (*NodeVersion, error) {
	nodeVersion, err := nodeVersionFromString(version)
	if err != nil {
		return nil, err
	}
	nodeVersion.IsMock = isMock

	return nodeVersion, nil
}

func nodeVersionFromString(version string) (*NodeVersion, error) {
	vSplit := strings.Split(version, ".")
	lenSplit := len(vSplit)
	if lenSplit == 0 {
		return nil, fmt.Errorf("must provide at least a major version")
	}

	var err error
	nodeVersion := NodeVersion{}
	nodeVersion.Major, err = strconv.Atoi(vSplit[0])
	if err != nil {
		return nil, fmt.Errorf("major version is not a valid integer")
	}
	if lenSplit == 1 {
		return &nodeVersion, nil
	}

	nodeVersion.Minor, err = strconv.Atoi(vSplit[1])
	if err != nil {
		return nil, fmt.Errorf("minor version is not a valid integer")
	}
	if lenSplit == 2 {
		return &nodeVersion, nil
	}

	nodeBuild := strings.Split(vSplit[2], "-")
	nodeVersion.Patch, err = strconv.Atoi(nodeBuild[0])
	if err != nil {
		return nil, fmt.Errorf("patch version is not a valid integer")
	}
	if len(nodeBuild) == 1 {
		return &nodeVersion, nil
	}

	buildEdition := strings.Split(nodeBuild[1], "-")
	nodeVersion.Build, err = strconv.Atoi(buildEdition[0])
	if err != nil {
		edition, modifier, err := editionModifierFromString(buildEdition[0])
		if err != nil {
			return nil, err
		}
		nodeVersion.Edition = edition
		nodeVersion.Modifier = modifier

		return &nodeVersion, nil
	}
	if len(buildEdition) == 1 {
		return &nodeVersion, nil
	}

	edition, modifier, err := editionModifierFromString(buildEdition[1])
	if err != nil {
		return nil, err
	}
	nodeVersion.Edition = edition
	nodeVersion.Modifier = modifier

	return &nodeVersion, nil
}

func editionModifierFromString(editionModifier string) (NodeEdition, string, error) {
	split := strings.Split(editionModifier, "-")
	editionStr := strings.ToLower(split[0])
	var edition NodeEdition
	var modifier string
	if editionStr == "enterprise" {
		edition = EnterpriseNodeEdition
	} else if editionStr == "community" {
		edition = CommunityNodeEdition
	} else if editionStr == "dp" {
		modifier = editionStr
	} else if editionStr == "protostellar" {
		edition = ProtostellarNodeEdition
	} else {
		return 0, "", errors.New("Unrecognised edition or modifier: " + editionStr)
	}
	if len(split) == 1 {
		return edition, modifier, nil
	}

	return edition, strings.ToLower(split[1]), nil
}
