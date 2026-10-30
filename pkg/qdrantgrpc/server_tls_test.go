package qdrantgrpc

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func TestServerTLS(t *testing.T) {
	serverTLS, clientTLS, clientMTLS := newGRPCTestTLSConfigs(t)

	t.Run("TLS rejects plaintext and accepts trusted clients", func(t *testing.T) {
		srv := startTLSTestServer(t, serverTLS)

		plaintext := dialGRPCTestClient(t, srv.Addr(), insecure.NewCredentials())
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := qpb.NewCollectionsClient(plaintext).List(ctx, &qpb.ListCollectionsRequest{})
		require.Error(t, err)

		secure := dialGRPCTestClient(t, srv.Addr(), credentials.NewTLS(clientTLS))
		ctx, cancel = context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err = qpb.NewCollectionsClient(secure).List(ctx, &qpb.ListCollectionsRequest{})
		require.NoError(t, err)
	})

	t.Run("mTLS requires a trusted client certificate", func(t *testing.T) {
		mtlsServer := serverTLS.Clone()
		mtlsServer.ClientAuth = tls.RequireAndVerifyClientCert
		mtlsServer.ClientCAs = serverTLS.RootCAs
		srv := startTLSTestServer(t, mtlsServer)

		withoutCertificate := dialGRPCTestClient(t, srv.Addr(), credentials.NewTLS(clientTLS))
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := qpb.NewCollectionsClient(withoutCertificate).List(ctx, &qpb.ListCollectionsRequest{})
		require.Error(t, err)

		withCertificate := dialGRPCTestClient(t, srv.Addr(), credentials.NewTLS(clientMTLS))
		ctx, cancel = context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err = qpb.NewCollectionsClient(withCertificate).List(ctx, &qpb.ListCollectionsRequest{})
		require.NoError(t, err)
	})
}

func startTLSTestServer(t *testing.T, tlsConfig *tls.Config) *Server {
	t.Helper()
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	databaseManager, err := multidb.NewDatabaseManager(t.Context(), base, nil)
	require.NoError(t, err)

	config := DefaultConfig()
	config.ListenAddr = "127.0.0.1:0"
	config.EnableReflection = false
	config.TLSConfig = tlsConfig

	server, err := NewServerWithDatabaseManager(config, databaseManager, base, nil, nil)
	require.NoError(t, err)
	require.NoError(t, server.Start())
	t.Cleanup(server.Stop)
	return server
}

func dialGRPCTestClient(t *testing.T, address string, transport credentials.TransportCredentials) *grpc.ClientConn {
	t.Helper()
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(transport))
	require.NoError(t, err)
	fixtureowner.Own(t, connection)
	return connection
}

func newGRPCTestTLSConfigs(t *testing.T) (*tls.Config, *tls.Config, *tls.Config) {
	t.Helper()
	now := time.Now()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "NornicDB test CA"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	ca, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	serverCertificate := issueGRPCTestCertificate(t, ca, caKey, 2, "localhost", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	clientCertificate := issueGRPCTestCertificate(t, ca, caKey, 3, "client", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
	roots := x509.NewCertPool()
	roots.AddCert(ca)

	serverTLS := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{serverCertificate},
		RootCAs:      roots,
	}
	clientTLS := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: "localhost"}
	clientMTLS := clientTLS.Clone()
	clientMTLS.Certificates = []tls.Certificate{clientCertificate}
	return serverTLS, clientTLS, clientMTLS
}

func issueGRPCTestCertificate(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, serial int64, commonName string, usages []x509.ExtKeyUsage) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: commonName},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  usages,
	}
	if commonName == "localhost" {
		template.DNSNames = []string{"localhost"}
		template.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	}
	certificateDER, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
	require.NoError(t, err)
	return tls.Certificate{Certificate: [][]byte{certificateDER, ca.Raw}, PrivateKey: key}
}
