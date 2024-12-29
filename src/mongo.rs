use anyhow::Result;
use mongodb::bson::{doc, Document};

/// Checks if a document for the given reference and council exists in the given collection
///
/// # Arguments
///
/// * `reference` - The reference number to search for
/// * `council` - The council to search in
/// * `collection` - The collection to search in
///
/// # Returns
///
/// A `Result` which is `Ok` containing a `Document` if the document exists,
/// or `None` if the document does not exist.
pub async fn check_reference(
    reference: &str,
    council: &str,
    collection: &mongodb::Collection<Document>,
) -> Result<Option<Document>> {
    let filter = doc! {
        "council": council,
        "summary.reference": reference,
    };

    let result: Option<Document> = collection.find_one(filter).await?;

    Ok(result)
}

/// Checks if a document for the given reference and council exists in the given collection
/// and if it has a decision notice document attached.
///
/// # Arguments
///
/// * `reference` - The reference number to search for
/// * `council` - The council to search in
/// * `collection` - The collection to search in
///
/// # Returns
///
/// A `Result` which is `Ok` containing a `Document` if the document exists and has a
/// decision notice document, or `None` if the document does not exist or does not have a
/// decision notice document.
pub async fn check_decision_exisits(
    reference: &str,
    council: &str,
    collection: &mongodb::Collection<Document>,
) -> Result<Option<Document>> {
    let filter = doc! {
        "council": council,
        "summary.reference": reference,
        "documents": {
            "$elemMatch": {
                "doc_type": "decision_notice"
            }
        }
    };

    let result: Option<Document> = collection.find_one(filter).await?;

    Ok(result)
}

/// Send data to the collection. If `update` is true, it will update
/// the existing document with the given `reference_id` and `council`.
/// If `update` is false, it will insert a new document into the collection.
///
/// # Arguments
///
/// * `reference_id` - The reference number to search for
/// * `council` - The council to search in
/// * `data` - The document to insert or update
/// * `collection` - The collection to insert or update into
/// * `update` - Whether to update an existing document or insert a new one
///
/// # Returns
///
/// A `Result` which is `Ok` if the operation was successful, or `Err` if the operation failed
pub async fn send_data(
    reference_id: &str,
    council: &str,
    data: Document,
    collection: &mongodb::Collection<Document>,
    update: bool,
) -> Result<()> {
    if update {
        let filter = doc! {
            "council": council,
            "summary.reference": reference_id,
        };
        let result = collection.update_one(filter, data).await?;
        if result.matched_count == 0 {
            println!("Document not found");
        }
    } else {
        collection.insert_one(data).await?;
    }

    Ok(())
}
