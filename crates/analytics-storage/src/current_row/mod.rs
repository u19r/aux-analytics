mod attributes;
mod contracts;
mod cursor;
mod projection;
mod readers;

pub use contracts::{
    AuxStorageCurrentRowClient, AuxStorageCurrentRowPageReader, AuxStorageCurrentRowScanRequest,
    DynamoDbCurrentRowClient, DynamoDbCurrentRowPageReader, DynamoDbCurrentRowScanRequest,
    DynamoDbCurrentRowScanResponse,
};

#[cfg(test)]
mod current_row_aux_storage_tests;

#[cfg(test)]
mod current_row_dynamodb_tests;

#[cfg(test)]
mod test_support;

#[cfg(test)]
mod projection_tests;
