# optd

optd (pronounced as op-dee) is a database optimizer framework. It is a cost-based optimizer that searches the plan space using the rules that the user defines and derives the optimal plan based on the cost model and the physical properties.

The primary objective of optd is to explore the potential challenges involved in effectively implementing a cost-based optimizer for real-world production usage. optd implements the Columbia Cascades optimizer framework based on [Yongwen Xu's master's thesis](https://15721.courses.cs.cmu.edu/spring2019/papers/22-optimizer1/xu-columbia-thesis1998.pdf). Besides cascades, optd also provides a heuristics optimizer implementation for testing purpose.

The other key objective is to ensure that the optimizer framework is flexible, enabling it to support adaptive query optimization and execution. optd supports adaptive query optimization (aka. reoptimization). It executes a query, captures runtime information, and utilizes this data to guide subsequent plan space searches and cost model estimations. This progressive optimization approach ensures that queries are continuously improved, and allows the optimizer to explore a large plan space.

Currently, optd is integrated into Apache Arrow Datafusion as a physical optimizer. It receives the logical plan from Datafusion, implements various physical optimizations (e.g., determining the join order), and subsequently converts it back into the Datafusion physical plan for execution.

optd is a research project and is still evolving. It should not be used in production. The code is licensed under MIT.

## Get Started

There are two demos you can run with optd. More information available in the [docs](docs/src/).

```
cargo run --release --bin optd-adaptive-tpch-q8
cargo run --release --bin optd-adaptive-three-join
```

You can also run the Datafusion cli to interactively experiment with optd.

```
cargo run --bin datafusion-optd-cli
```

## Documentation

The documentation is available in the mdbook format in the [docs](docs/src) directory.

## Structure

* `datafusion-optd-cli`: patched Apache Arrow Datafusion cli that calls into optd
* `datafusion-optd-bridge`: implementation of Apache Arrow Datafusion query planner as a bridge between optd and Apache Arrow Datafusion.
* `optd-core`: the core framework of optd.
* `optd-datafusion-repr`: representation of Apache Arrow Datafusion plan nodes in optd.
* `optd-adaptive-demo`: demo of adaptive optimization capabilities of optd. More information available in the [docs](docs/src/).
* `optd-sqlplannertest`: planner test of optd.
