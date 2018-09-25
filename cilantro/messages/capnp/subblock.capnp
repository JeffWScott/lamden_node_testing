@0xab5e7866e64c6d4d;

using T = import "transaction.capnp";

struct SubBlock {
    merkleRoot @0 :Data;
    signatures @1: List(Data);
    merkleLeaves @2: List(Data);
    subBlockIdx @3: UInt8;
}

struct SubBlockContender {
    resultHash @0 :Data;
    inputHash @1 :Data;
    merkleLeaves @2: List(Data);
    signature @3 :Data;
    transactions @4: List(T.TransactionData);
    subBlockIdx @5: UInt8;
}
