package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// imdsFake fails the first failFirst calls, then returns document. It records how long each attempt's context
// had left, so a test can check that every attempt gets its own deadline.
type imdsFake struct {
	failFirst int
	document  *imds.GetInstanceIdentityDocumentOutput
	calls     int
	remaining []time.Duration
}

func (f *imdsFake) GetInstanceIdentityDocument(ctx context.Context, _ *imds.GetInstanceIdentityDocumentInput,
	_ ...func(*imds.Options)) (*imds.GetInstanceIdentityDocumentOutput, error) {
	f.calls++
	if deadline, ok := ctx.Deadline(); ok {
		f.remaining = append(f.remaining, time.Until(deadline))
	}
	if f.calls <= f.failFirst {
		return nil, errors.New("request send failed: dial tcp 169.254.169.254:80: i/o timeout")
	}
	return f.document, nil
}

func fastIMDSRetries(t *testing.T) {
	t.Helper()
	minBackoff, maxBackoff, attemptTimeout := imdsRetryMinBackoff, imdsRetryMaxBackoff, imdsAttemptTimeout
	imdsRetryMinBackoff, imdsRetryMaxBackoff, imdsAttemptTimeout = time.Millisecond, time.Millisecond, time.Second
	t.Cleanup(func() {
		imdsRetryMinBackoff, imdsRetryMaxBackoff, imdsAttemptTimeout = minBackoff, maxBackoff, attemptTimeout
	})
}

func exampleDocument() *imds.GetInstanceIdentityDocumentOutput {
	return &imds.GetInstanceIdentityDocumentOutput{
		InstanceIdentityDocument: imds.InstanceIdentityDocument{
			Region:     "us-west-2",
			InstanceID: "i-11111111111111111",
		},
	}
}

func TestInitializeAWSRetriesIMDS(t *testing.T) {
	fastIMDSRetries(t)
	fake := &imdsFake{failFirst: 3, document: exampleDocument()}

	err := initializeAWS(context.Background(), aws.Config{}, fake)

	require.NoError(t, err)
	assert.Equal(t, 4, fake.calls)
	assert.Equal(t, "us-west-2", region)
	assert.Equal(t, "i-11111111111111111", instanceID)
	assert.NotNil(t, cwlClient)
}

func TestInitializeAWSGivesUpAfterMaxRetries(t *testing.T) {
	fastIMDSRetries(t)
	fake := &imdsFake{failFirst: imdsMaxRetries + 1, document: exampleDocument()}

	err := initializeAWS(context.Background(), aws.Config{}, fake)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get instance identity document")
	assert.Equal(t, imdsMaxRetries, fake.calls)
}

func TestInitializeAWSDeadlinePerAttempt(t *testing.T) {
	fastIMDSRetries(t)
	fake := &imdsFake{failFirst: 4, document: exampleDocument()}

	require.NoError(t, initializeAWS(context.Background(), aws.Config{}, fake))

	// Every attempt, including the ones after a failure, starts with a fresh deadline rather than inheriting
	// what is left of an earlier one.
	require.Len(t, fake.remaining, 5)
	for _, remaining := range fake.remaining {
		assert.Greater(t, remaining, imdsAttemptTimeout/2)
		assert.LessOrEqual(t, remaining, imdsAttemptTimeout)
	}
}

func TestInitializeAWSNilDocument(t *testing.T) {
	fastIMDSRetries(t)
	fake := &imdsFake{}

	err := initializeAWS(context.Background(), aws.Config{}, fake)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "no instance identity document")
}

func TestInitializeAWSStopsOnCancel(t *testing.T) {
	fastIMDSRetries(t)
	imdsRetryMinBackoff, imdsRetryMaxBackoff = time.Minute, time.Minute
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	fake := &imdsFake{failFirst: 1, document: exampleDocument()}

	start := time.Now()
	err := initializeAWS(ctx, aws.Config{}, fake)

	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, fake.calls)
	assert.Less(t, time.Since(start), 10*time.Second)
}
