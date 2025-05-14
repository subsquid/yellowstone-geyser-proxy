# yellowstone-geyser-proxy

`sqd-yellowstone-geyser-proxy(1)` allows to receive new Solana blocks via JavaScript friendly [JSON-RPC websocket subscription](https://geth.ethereum.org/docs/interacting-with-geth/rpc/pubsub). 

It works as a proxy for [Yellowstone gRPC Geyser plugin](https://github.com/rpcpool/yellowstone-grpc).

At SQD, we use it as a temporary workaround before we migrate our entire data ingestion stack from JavaScript to Rust.

## Why not to use gRPC directly?

* Yellowstone gRPC delivers account ids in a binary form, 
but you want base58 encodings most of the time. Base58 encodings are too expensive to produce on JS side.
* Yellowstone gRPC whole block subscription does not allow to exclude vote transactions.
* JavaScript gRPC stack is heavy and ugly.

## Usage

Only single subscription kind is supported, that delivers the whole block data.

* `geyser_blockSubscribe` - subscription method (no parameters)
* `geyser_blockNotification` - data message
* `geyser_blockUnsubscribe` - subscription cancellation.

Format of the data message is `{"slot": <number>, "block": <GetBlock>}`,
where `GetBlock` is akin to [getBlock result](https://solana.com/ru/docs/rpc/http/getblock) except:

* Each transaction is annotated with additional `_index` property to identify 
its original position within a block (in case vote transactions where excluded).
* All integer fields, that can potentially overflow JavaScript safe integer are replaced with number strings.

## Setup

```
Usage: sqd-yellowstone-geyser-proxy [OPTIONS] --geyser-url <URL>

Options:
      --geyser-url <URL>               Yellowstone gRPC service
      --geyser-x-token <TOKEN>         add X-Token header to gRPC requests
      --geyser-x-access-token <TOKEN>  add X-Access-Token header to gRPC requests
      --with-votes                     disable removal of vote transactions
      --port <PORT>                    [default: 3000]
      --mapping-threads <N>            number of threads to use for data transformation

```