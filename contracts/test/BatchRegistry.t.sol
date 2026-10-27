// SPDX-License-Identifier: MIT
pragma solidity 0.8.30;

import {BatchRegistry} from "../src/BatchRegistry.sol";

interface Vm {
    function expectRevert(bytes4 selector) external;
    function prank(address sender) external;
    function startPrank(address sender) external;
    function stopPrank() external;
}

contract BatchRegistryTest {
    Vm private constant vm = Vm(address(uint160(uint256(keccak256("hevm cheat code")))));
    BatchRegistry private registry;
    bytes32 private constant STREAM = keccak256("synthetic-stream");
    address private constant PUBLISHER = address(0xBEEF);

    function setUp() public {
        registry = new BatchRegistry();
        registry.registerStream(STREAM, PUBLISHER);
    }

    function testAppendAndReadBack() public {
        bytes32 commitment = keccak256("batch-zero");
        vm.expectRevert(BatchRegistry.Unauthorized.selector);
        registry.appendBatch(STREAM, 0, bytes32(0), commitment, 1, 3, 3);

        vm.prank(PUBLISHER);
        registry.appendBatch(STREAM, 0, bytes32(0), commitment, 1, 3, 3);
        BatchRegistry.Batch memory stored = registry.getBatch(STREAM, 0);
        require(stored.commitment == commitment, "commitment");
        require(stored.firstSequence == 1 && stored.lastSequence == 3 && stored.recordCount == 3, "range");
        require(registry.batchCount(STREAM) == 1 && registry.headCommitment(STREAM) == commitment, "head");
    }

    function testRejectDuplicateSkippedAndWrongPredecessor() public {
        vm.startPrank(PUBLISHER);
        registry.appendBatch(STREAM, 0, bytes32(0), keccak256("a"), 1, 1, 1);
        vm.expectRevert(BatchRegistry.InvalidBatchNumber.selector);
        registry.appendBatch(STREAM, 0, keccak256("a"), keccak256("b"), 2, 2, 1);
        vm.expectRevert(BatchRegistry.InvalidBatchNumber.selector);
        registry.appendBatch(STREAM, 2, keccak256("a"), keccak256("b"), 2, 2, 1);
        vm.expectRevert(BatchRegistry.InvalidPredecessor.selector);
        registry.appendBatch(STREAM, 1, bytes32(0), keccak256("b"), 2, 2, 1);
        vm.stopPrank();
    }

    function testRejectInvalidCommitmentAndSequenceRange() public {
        vm.startPrank(PUBLISHER);
        vm.expectRevert(BatchRegistry.InvalidCommitment.selector);
        registry.appendBatch(STREAM, 0, bytes32(0), bytes32(0), 1, 1, 1);
        vm.expectRevert(BatchRegistry.InvalidSequenceRange.selector);
        registry.appendBatch(STREAM, 0, bytes32(0), keccak256("a"), 1, 3, 2);
        vm.expectRevert(BatchRegistry.InvalidSequenceRange.selector);
        registry.appendBatch(STREAM, 0, bytes32(0), keccak256("a"), 1, 1, 0);
        vm.expectRevert(BatchRegistry.InvalidSequenceRange.selector);
        registry.appendBatch(STREAM, 0, bytes32(0), keccak256("a"), 2, 2, 1);
        vm.stopPrank();
    }

    function testRejectSequenceGapAcrossBatches() public {
        vm.startPrank(PUBLISHER);
        registry.appendBatch(STREAM, 0, bytes32(0), keccak256("a"), 1, 2, 2);
        vm.expectRevert(BatchRegistry.InvalidSequenceRange.selector);
        registry.appendBatch(STREAM, 1, keccak256("a"), keccak256("b"), 4, 4, 1);
        registry.appendBatch(STREAM, 1, keccak256("a"), keccak256("c"), 3, 3, 1);
        vm.stopPrank();
    }

    function testOwnerEnrollmentAndFuturePublisherRotation() public {
        address nextPublisher = address(0xCAFE);
        registry.setPublisher(STREAM, nextPublisher);
        vm.expectRevert(BatchRegistry.Unauthorized.selector);
        vm.prank(PUBLISHER);
        registry.setPublisher(STREAM, PUBLISHER);

        vm.prank(nextPublisher);
        registry.appendBatch(STREAM, 0, bytes32(0), keccak256("a"), 1, 1, 1);
        require(registry.getBatch(STREAM, 0).commitment == keccak256("a"), "rotated publisher");
    }

    function testFuzzContiguousRanges(uint64 first, uint64 count) public {
        if (first != 1 || count == 0 || count > registry.MAX_BATCH_RECORDS()) return;
        uint64 last = uint64(uint256(first) + count - 1);
        vm.prank(PUBLISHER);
        registry.appendBatch(STREAM, 0, bytes32(0), keccak256("fuzz"), first, last, count);
        require(registry.getBatch(STREAM, 0).recordCount == count, "count");
    }
}
