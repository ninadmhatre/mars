# ma.rs

----

ma.rs is a small project about fetching prices and building a simple Linear regression models.

### Structure

```text
- src/
    - lib.rs
    - pipelines/
        - mod.rs
        - vendor/
            - mod.rs
            - data_bento.rs
            - yahoo_finance.rs
    - crafter/
        - mod.rs
        - dal.rs
        - nodes.rs
        - builder.rs
        - dag.rs
        - utils.rs
    - models/
        - mod.rs
        - executor.rs        
```

### Workflow

#### Pipelines

```rust
use mars::pipelines::FetchMode;
use mars::pipelines::vendor::DataBento;
use mars::crafter::dal::{RefData, PxData};

ref_data = RefData::new();
client = DataBento::new(FetchMode::Incremental);

client.fetch(ref_data.active_symbols()); // get since 2023-01-01 or last fetched which is stored locally.
client.persist();  // save to sqlite
```

#### Crafter

```rust
use ma.rs::crafter::dtypes::{PxCol};
use ma.rs::crafter::Crafter;
use ma.rs::crafter::nodes::{StockPx, IndexPx, Return};

crafter = Crafter::new();

aapl_px = crafter.add_node(StockPx::new("AAPL", PxCol::Close));
msft_px = crafter.add_node(StockPx::new("MSFT", PxCol::Close));
spx_px = crafter.add_node(IndexPx::new("SPX", PxCol::Close));

aapl_return = crafter.add_node(Return::new(aapl_px));
msft_return = crafter.add_node(Return::new(msft_px));
spx_return = crafter.add_node(Return::new(spx_px));

X_train, X_test = crafter.add_node(TTSplit::new([spx_return], test_size=0.2));
y_train, y_test = crafter.add_node(TTSplit::new([aapl_return, msft_return], test_size=0.2)); ;

crafter.set_regressor(LinearRegression());

crafter.fit(X_train, y_train);
crafter.metrics(X_test, y_test);

crafter.serialize("model.json");
// crafter.deserialize("model.json");
```

### Models

```rust
use ma.rs::models::{Executor, Serde};
use ma.rs::plotter::Plotter;  // optional

executor = Executor::new("model.json");
result =executor.run(from:"2023-01-01", to:"2023-01-31");

serde = Serde::new();


model_id = serde.serialize(result, "aapl_msft_vs_spx")  // save to db
result = serder.deserialize(model_id);

plotter1 = Plotter::from_result(result).plot();
plotter2 = Plotter::from_model_id(model_id).plot();
```