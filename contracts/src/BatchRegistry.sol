// SPDX-License-Identifier: MIT
pragma solidity 0.8.30;

/// @notice Stores ordered, opaque commitments for DEINOPIS integrity batches.
/// @dev Manifests and event data remain off-chain. This contract has no upgrade or withdrawal path.
contract BatchRegistry {
    uint64 public constant MAX_BATCH_RECORDS = 50_000;

    struct Batch {
        bytes32 commitment;
        uint64 firstSequence;
        uint64 lastSequence;
        uint64 recordCount;
    }

    address public immutable owner;
    mapping(bytes32 streamId => address publisher) public publisherOf;
    mapping(bytes32 streamId => uint64 count) public batchCount;
    mapping(bytes32 streamId => bytes32 head) public headCommitment;
    mapping(bytes32 streamId => mapping(uint64 batchNo => Batch)) private batches;

    event StreamRegistered(bytes32 indexed streamId, address indexed publisher);
    event PublisherChanged(bytes32 indexed streamId, address indexed oldPublisher, address indexed newPublisher);
    event BatchAppended(
        bytes32 indexed streamId,
        uint64 indexed batchNo,
        bytes32 commitment,
        uint64 firstSequence,
        uint64 lastSequence,
        uint64 recordCount
    );

    error Unauthorized();
    error InvalidStream();
    error StreamAlreadyRegistered();
    error StreamNotRegistered();
    error InvalidPublisher();
    error InvalidBatchNumber();
    error InvalidPredecessor();
    error InvalidCommitment();
    error InvalidSequenceRange();

    constructor() {
        owner = msg.sender;
    }

    function registerStream(bytes32 streamId, address publisher) external {
        if (msg.sender != owner) revert Unauthorized();
        if (streamId == bytes32(0)) revert InvalidStream();
        if (publisherOf[streamId] != address(0)) revert StreamAlreadyRegistered();
        if (publisher == address(0)) revert InvalidPublisher();
        publisherOf[streamId] = publisher;
        emit StreamRegistered(streamId, publisher);
    }

    /// @notice Changes only who may append future batches; prior records are immutable.
    function setPublisher(bytes32 streamId, address publisher) external {
        if (msg.sender != owner) revert Unauthorized();
        address oldPublisher = publisherOf[streamId];
        if (oldPublisher == address(0)) revert StreamNotRegistered();
        if (publisher == address(0)) revert InvalidPublisher();
        publisherOf[streamId] = publisher;
        emit PublisherChanged(streamId, oldPublisher, publisher);
    }

    function appendBatch(
        bytes32 streamId,
        uint64 batchNo,
        bytes32 previousCommitment,
        bytes32 commitment,
        uint64 firstSequence,
        uint64 lastSequence,
        uint64 recordCount
    ) external {
        if (msg.sender != publisherOf[streamId] || msg.sender == address(0)) revert Unauthorized();
        if (batchNo != batchCount[streamId] || batchNo == type(uint64).max) revert InvalidBatchNumber();
        if (previousCommitment != headCommitment[streamId]) revert InvalidPredecessor();
        if (commitment == bytes32(0)) revert InvalidCommitment();
        if (
            recordCount == 0 || recordCount > MAX_BATCH_RECORDS || firstSequence == 0 || lastSequence < firstSequence
                || uint256(lastSequence) - uint256(firstSequence) + 1 != recordCount
        ) {
            revert InvalidSequenceRange();
        }
        if (batchNo == 0) {
            if (firstSequence != 1) revert InvalidSequenceRange();
        } else {
            uint64 priorLast = batches[streamId][batchNo - 1].lastSequence;
            if (priorLast == type(uint64).max || firstSequence != priorLast + 1) revert InvalidSequenceRange();
        }

        batches[streamId][batchNo] = Batch(commitment, firstSequence, lastSequence, recordCount);
        batchCount[streamId] = batchNo + 1;
        headCommitment[streamId] = commitment;
        emit BatchAppended(streamId, batchNo, commitment, firstSequence, lastSequence, recordCount);
    }

    function getBatch(bytes32 streamId, uint64 batchNo) external view returns (Batch memory) {
        if (batchNo >= batchCount[streamId]) revert InvalidBatchNumber();
        return batches[streamId][batchNo];
    }
}
