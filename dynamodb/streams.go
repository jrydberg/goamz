package dynamodb

import (
	"encoding/json"
	"fmt"
	"github.com/crowdmob/goamz/dynamodb/dynamizer"
)

type ShardIteratorType string
type StreamStatus string
type StreamViewType string

const (
	// Start reading exactly from the position denoted by a specific sequence number.
	ShardIteratorAtSequenceNumber ShardIteratorType = "AT_SEQUENCE_NUMBER"

	// Start reading right after the position denoted by a specific sequence number.
	ShardIteratorAfterSequenceNumber ShardIteratorType = "AFTER_SEQUENCE_NUMBER"

	// Start reading at the last untrimmed record in the shard in the system,
	// which is the oldest data record in the shard.
	ShardIteratorTrimHorizon ShardIteratorType = "TRIM_HORIZON"

	// Start reading just after the most recent record in the shard,
	// so that you always read the most recent data in the shard.
	ShardIteratorLatest ShardIteratorType = "LATEST"

	// The stream is being created. Upon receiving a CreateStream request,
	// Amazon Kinesis immediately returns and sets StreamStatus to CREATING.
	StreamStatusCreating StreamStatus = "CREATING"

	// The stream is being deleted. After a DeleteStream request,
	// the specified stream is in the DELETING state until Amazon Kinesis completes the deletion.
	StreamStatusDeleting StreamStatus = "DELETING"

	// The stream exists and is ready for read and write operations or deletion.
	// You should perform read and write operations only on an ACTIVE stream.
	StreamStatusActive StreamStatus = "ACTIVE"

	// Shards in the stream are being merged or split.
	// Read and write operations continue to work while the stream is in the UPDATING state.
	StreamStatusUpdating StreamStatus = "UPDATING"
)

const (
	// Only the key attributes of items that were modified in the DynamoDB table.
	StreamViewTypeKeysOnly StreamViewType = "KEYS_ONLY"

	// Entire item from the table, as it appeared after they were modified.
	StreamViewTypeNewImage StreamViewType = "NEW_IMAGE"

	// Entire item from the table, as it appeared before they were modified.
	StreamViewTypeOldImage StreamViewType = "OLD_IMAGE"

	// Both the new and the old images of the items from the table.
	StreamViewTypeNewAndOldImage StreamViewType = "NEW_AND_OLD_IMAGE"
)

// The range of possible sequence numbers for the shard.
type SequenceNumberRangeT struct {
	EndingSequenceNumber   string
	StartingSequenceNumber string
}

func (s SequenceNumberRangeT) String() string {
	return fmt.Sprintf("{EndingSequenceNumber: %s, StartingSequenceNumber: %s}\n",
		s.EndingSequenceNumber, s.StartingSequenceNumber)
}

// A uniquely identified group of data records in an Amazon Kinesis stream.
type ShardT struct {
	ParentShardId       string
	SequenceNumberRange SequenceNumberRangeT
	ShardId             string
}

// Description of a Stream
type StreamDescriptionT struct {
	KeySchema            []KeySchemaT
	LastEvaluatedShardId string
	Shards               []ShardT
	StreamARN            string
	StreamID             string
	StreamStatus         StreamStatus
	TableName            string
}

// Represents the output of a DescribeStream operation.
type describeStreamResponse struct {
	StreamDescription StreamDescriptionT
}

func (t *Table) DescribeStream(streamId string) (*StreamDescriptionT, error) {
	q := NewEmptyQuery()
	q.AddDescribeStreamRequest(streamId)

	jsonResponse, err := t.Server.queryServer(target("DescribeStream"), q)
	if err != nil {
		return nil, err
	}

	var r describeStreamResponse
	err = json.Unmarshal(jsonResponse, &r)
	if err != nil {
		return nil, err
	}

	return &r.StreamDescription, nil
}

type listStreamsResponse struct {
	LastEvaluatedStreamId string
	StreamIds             []string
}

func (t *Table) ListStreams() ([]string, error) {
	q := NewEmptyQuery()
	q.addTableByName(t.Name)

	jsonResponse, err := t.Server.queryServer(target("ListStreams"), q)
	if err != nil {
		return nil, err
	}

	var r listStreamsResponse
	err = json.Unmarshal(jsonResponse, &r)
	if err != nil {
		return nil, err
	}

	return r.StreamIds, nil
}

type getShardIteratorResponse struct {
	ShardIterator string
}

func (t *Table) GetShardIteratorWithSeqNumber(streamId, shardId string, shardIteratorType ShardIteratorType, seqNumber string) (string, error) {
	q := NewEmptyQuery()
	q.AddGetShardIteratorRequest(streamId, shardId, string(shardIteratorType), seqNumber)

	jsonResponse, err := t.Server.queryServer(target("GetShardIterator"), q)
	if err != nil {
		return "", err
	}

	var r getShardIteratorResponse
	err = json.Unmarshal(jsonResponse, &r)
	if err != nil {
		return "", err
	}

	return r.ShardIterator, nil
}

func (t *Table) GetShardIterator(streamId, shardId string, shardIteratorType ShardIteratorType) (string, error) {
	return t.GetShardIteratorWithSeqNumber(streamId, shardId, shardIteratorType, "")
}

type StreamRecordT struct {
	Keys           dynamizer.DynamoItem
	NewImage       dynamizer.DynamoItem
	OldImage       dynamizer.DynamoItem
	SequenceNumber string
	SizeBytes      int64
	StreamViewType string
}

type RecordT struct {
	AwsRegion    string        `json:awsRegion`
	DynamoDB     StreamRecordT `json:dynamodb`
	EventID      string        `json:eventID`
	EventName    string        `json:eventName`
	EventSource  string        `json:eventSource`
	EventVersion string        `json:eventVersion`
}

type getRecordsResponse struct {
	NextShardIterator string
	Records           []*RecordT
}

func (t *Table) GetRecordsWithLimit(shardIterator string, limit int) (string, []*RecordT, error) {
	q := NewEmptyQuery()
	q.AddGetRecordsRequest(shardIterator, limit)

	jsonResponse, err := t.Server.queryServer(target("GetRecords"), q)
	if err != nil {
		return "", nil, err
	}

	var r getRecordsResponse
	err = json.Unmarshal(jsonResponse, &r)
	if err != nil {
		return "", nil, err
	}

	return r.NextShardIterator, r.Records, nil
}

func (t *Table) GetRecords(shardIterator string) (string, []*RecordT, error) {
	return t.GetRecordsWithLimit(shardIterator, 0)
}
