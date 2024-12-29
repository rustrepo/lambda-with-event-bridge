use crate::mongo::{check_decision_exisits, check_reference};
use crate::{mongo, s3upload};

use anyhow::Result;
use indicatif::{ProgressBar, ProgressStyle};
use mongodb::bson::{doc, Document};
use reqwest::Client;
use scraper::{Html, Selector};
use std::collections::HashMap;
use std::{thread, time::Duration};


const COUNTY: &str = "Leeds";
const BASE_URL: &str = "https://publicaccess.leeds.gov.uk";

const WEEK_SELECTOR: &str = r#"select[name="week"] > option:first-of-type"#;
const TOKEN_SELECTOR: &str = r#"input[name="org.apache.struts.taglib.html.TOKEN"]"#;
const CSRF_SELECTOR: &str = r#"input[name="_csrf"]"#;
const SUMMARY_SELECTOR: &str = r#"ul#searchresults > li.searchresult > a.summaryLink"#;
const DOCS_SELECTOR: &str = r#"tr > td:nth-child(3)"#;
const DESCRIPTION_SELECTOR: &str = r#"tr > td:nth-child(5)"#;
const DOCS_LINK_SELECTOR: &str = r#"tr > td:nth-child(6) > a"#;
const REFERENCE_ID_SELECTOR: &str = r#"div.addressCrumb > span.caseNumber"#;
const PAGINATION_SELECTOR: &str = r#"a.next"#;
const SIMPLE_DETAILS_TABLE_SELECTOR: &str = r#"table#simpleDetailsTable"#;
const FURTHER_INFORMATION_SELECTOR: &str = r#"table#applicationDetails"#;
const AGENTS_SELECTOR: &str = r#"table.agents"#;


fn parse_date(date_str: String) -> Option<String> {
    match chrono::NaiveDate::parse_from_str(&date_str, "%a %d %b %Y") {
        Ok(date) => Some(format!("{}", date.format("%Y-%m-%d"))),
        Err(_) => None,
    }
}


/// Extracts a list of links from a given URL.
///
/// This function sends a GET request to the specified URL, modifies it to
/// retrieve a print preview version, and then parses the HTML response to
/// extract various details including summary, further information, and agent
/// details. These details are collected into a BSON document, which includes
/// information such as reference numbers, application dates, addresses, and
/// proposal details among others.
///
/// # Arguments
///
/// * `client` - An instance of `Client` used to execute HTTP requests.
/// * `url` - A string slice representing the URL of the document to retrieve.
/// * `option` - A string slice representing the option to select on the page.
///
/// # Returns
///
/// A vector of strings representing the links extracted from the page.
pub async fn extract_links(client: &Client, url: &str, option: &str) -> Result<Vec<String>> {
    let html = client.get(url).send().await?.text().await?;
    let document = Html::parse_document(&html);

    let week = document
        .select(&Selector::parse(WEEK_SELECTOR).expect("Failed to parse selector"))
        .next()
        .ok_or("No options found")
        .expect("No options found")
        .value()
        .attr("value")
        .expect("No value found");

    let token = document
        .select(&Selector::parse(TOKEN_SELECTOR).expect("Token not found."))
        .next()
        .and_then(|e| e.value().attr("value"));

    let csrf = document
        .select(&Selector::parse(CSRF_SELECTOR).expect("csrf not found."))
        .next()
        .ok_or("No csrf input found")
        .expect("No csrf input found")
        .value()
        .attr("value")
        .expect("No csrf value found");

    let mut form_data = vec![
        ("_csrf", csrf),
        ("searchCriteria.parish", ""),
        ("searchCriteria.ward", ""),
        ("week", week),
        ("dateType", option),
        ("searchType", "Application"),
    ];
    if let Some(token) = token {
        form_data.push(("org.apache.struts.taglib.html.TOKEN", token));
    }

    let html = client
        .get(&format!(
            "{}{}",
            BASE_URL, "/online-applications/weeklyListResults.do?action=firstPage"
        ))
        .form(&form_data)
        .send()
        .await?
        .text()
        .await?;
    let document = Html::parse_document(&html);

    let token = document
        .select(&Selector::parse(TOKEN_SELECTOR).expect("Token not found."))
        .next()
        .and_then(|e| e.value().attr("value"));

    let csrf = document
        .select(&Selector::parse(CSRF_SELECTOR).expect("csrf not found."))
        .next()
        .ok_or("No csrf input found")
        .expect("No csrf input found")
        .value()
        .attr("value")
        .expect("No csrf value found");

    let mut form_data = vec![
        ("_csrf", csrf),
        ("searchCriteria.page", "1"),
        ("action", "page"),
        ("orderBy", "DateReceived"),
        ("orderByDirection", "Descending"),
        ("searchCriteria.resultsPerPage", "100"),
    ];
    if let Some(token) = token {
        form_data.push(("org.apache.struts.taglib.html.TOKEN", token));
    }

    let html = client
        .get(&format!(
            "{}{}",
            BASE_URL, "/online-applications/pagedSearchResults.do"
        ))
        .form(&form_data)
        .send()
        .await?
        .text()
        .await?;
    let document = Html::parse_document(&html);

    let mut links = document
        .select(&Selector::parse(SUMMARY_SELECTOR).expect("Failed to parse selector"))
        .map(|e| e.value().attr("href").unwrap_or_default().to_string())
        .collect::<Vec<_>>();

    println!("Total pages: {}", links.len());
    let mut next_page = document
        .select(&Selector::parse(PAGINATION_SELECTOR).expect("Failed to parse selector"))
        .next()
        .and_then(|e| e.value().attr("href").map(|s| s.to_string()));

    while let Some(page) = next_page {
        thread::sleep(Duration::from_secs(1));
        let html = client
            .get(&format!("{}{}", BASE_URL, page))
            .send()
            .await?
            .text()
            .await?;
        let document = Html::parse_document(&html);

        let new_links = document
            .select(&Selector::parse(SUMMARY_SELECTOR).expect("Failed to parse selector"))
            .map(|e| e.value().attr("href").unwrap_or_default().to_string())
            .collect::<Vec<_>>();
        println!("Total pages: {}", new_links.len());

        links.extend(new_links);
        next_page = document
            .select(&Selector::parse(PAGINATION_SELECTOR).expect("Failed to parse selector"))
            .next()
            .and_then(|e| e.value().attr("href").map(|s| s.to_string()));
    }
    Ok(links)
}

/// Fetches and parses document details from a given URL.
///
/// This function sends a GET request to the specified URL, modifies it to
/// retrieve a print preview version, and then parses the HTML response to
/// extract various details including summary, further information, and agent
/// details. These details are collected into a BSON document, which includes
/// information such as reference numbers, application dates, addresses, and
/// proposal details among others.
///
/// # Arguments
///
/// * `client` - An instance of `Client` used to execute HTTP requests.
/// * `url` - A string slice representing the URL of the document to retrieve.
///
/// # Returns
///
/// Returns a `Result` which is `Ok` containing a `Document` if successful,
/// or an error if the operation fails.
pub async fn get_document(client: &Client, url: &str) -> Result<Document> {
    let tr_selector = Selector::parse("tr").expect("Failed to parse selector");
    let td_selector = Selector::parse("td").expect("Failed to parse selector");
    let th_selector = Selector::parse("th").expect("Failed to parse selector");

    let print_preview_url = url.replace("=summary", "=printPreview");
    let html = client.get(print_preview_url).send().await?.text().await?;
    let document = Html::parse_document(&html);

    let mut summary = HashMap::new();
    let mut further_information = HashMap::new();
    let mut agents = HashMap::new();

    let table = document
        .select(&Selector::parse(SIMPLE_DETAILS_TABLE_SELECTOR).expect("Failed to parse selector"))
        .map(|e| e)
        .collect::<Vec<_>>();

    if let Some(tab) = table.get(0) {
        for row in tab.select(&tr_selector) {
            let th = row.select(&th_selector).next().expect("No th found");
            let td = row.select(&td_selector).next().expect("No td found");
            let key = th
                .text()
                .collect::<String>()
                .trim()
                .to_string()
                .to_lowercase()
                .replace(" ", "_");
            let value = td.text().collect::<String>().trim().to_string();
            summary.insert(key, value);
        }
    }
    if let Some(tab) = table.get(1) {
        for row in tab.select(&tr_selector) {
            let th = row.select(&th_selector).next().expect("No th found");
            let td = row.select(&td_selector).next().expect("No td found");
            let key = th
                .text()
                .collect::<String>()
                .trim()
                .to_string()
                .to_lowercase()
                .replace(" ", "_");
            let value = td.text().collect::<String>().trim().to_string();
            summary.insert(key, value);
        }
    }

    if let Some(table) = document
        .select(&Selector::parse(FURTHER_INFORMATION_SELECTOR).expect("Failed to parse selector"))
        .next()
    {
        for row in table.select(&tr_selector) {
            let th = row.select(&th_selector).next().expect("No th found");
            let td = row.select(&td_selector).next().expect("No td found");
            let key = th
                .text()
                .collect::<String>()
                .trim()
                .to_string()
                .to_lowercase()
                .replace(" ", "_");
            let value = td.text().collect::<String>().trim().to_string();
            further_information.insert(key, value);
        }
    }

    if let Some(table) = document
        .select(&Selector::parse(AGENTS_SELECTOR).expect("Failed to parse selector"))
        .next()
    {
        for row in table.select(&tr_selector) {
            let th = row.select(&th_selector).next().expect("No th found");
            let td = row.select(&td_selector).next().expect("No td found");
            let key = th
                .text()
                .collect::<String>()
                .trim()
                .to_string()
                .to_lowercase()
                .replace(" ", "_");
            let value = td.text().collect::<String>().trim().to_string();
            agents.insert(key, value);
        }
    }

    let document = doc! {
        "council": COUNTY.to_string(),
        "link": url.to_string().replace("=printPreview", "=summary"),
        "summary": {
            "reference": summary.get("reference").unwrap_or(&String::new()).to_string(),
            // "application_received": summary.get("application_received").unwrap_or(&String::new()).to_string(),
            "application_validated": summary.get("application_validated").unwrap_or(&String::new()).to_string(),
            "address": summary.get("address").unwrap_or(&String::new()).to_string(),
            "proposal": summary.get("proposal").unwrap_or(&String::new()).to_string(),
            "status": summary.get("status").unwrap_or(&String::new()).to_string(),
            "decision": summary.get("decision").unwrap_or(&String::new()).to_string(),
            "decision_issued_date": parse_date(summary.get("decision_issued_date").unwrap_or(&String::new()).to_string()),
            "appeal_status": summary.get("appeal_status").unwrap_or(&String::new()).to_string(),
            "appeal_decision": summary.get("appeal_decision").unwrap_or(&String::new()).to_string(),
            "application_validated_date": parse_date(summary.get("application_validated_date").unwrap_or(&String::new()).to_string()),
            // "actual_committee_date": parse_date(summary.get("actual_committee_date").unwrap_or(&String::new()).to_string()),
            // "neighbour_consultation_expiry_date": parse_date(summary.get("neighbour_consultation_expiry_date").unwrap_or(&String::new()).to_string()),
            // "standard_consultation_expiry_date": parse_date(summary.get("standard_consultation_expiry_date").unwrap_or(&String::new()).to_string()),
            // "latest_advertisement_expiry_date": parse_date(summary.get("latest_advertisement_expiry_date").unwrap_or(&String::new()).to_string()),
            // "latest_site_notice_expiry_date": parse_date(summary.get("latest_site_notice_expiry_date").unwrap_or(&String::new()).to_string()),
            // "application_expiry_date": summary.get("application_expiry_date").unwrap_or(&String::new()).to_string(),
            "agreed_expiry_date": parse_date(summary.get("agreed_expiry_date").unwrap_or(&String::new()).to_string()),
            "determination_deadline": parse_date(summary.get("determination_deadline").unwrap_or(&String::new()).to_string()),
        },
        "further_information": {
            "application_type": further_information.get("application_type").unwrap_or(&String::new()).to_string(),
            "actual_decision_level": further_information.get("actual_decision_level").unwrap_or(&String::new()).to_string(),
            "expected_decision_level": further_information.get("expected_decision_level").unwrap_or(&String::new()).to_string(),
            "parish": further_information.get("parish").unwrap_or(&String::new()).to_string(),
            "ward": further_information.get("ward").unwrap_or(&String::new()).to_string(),
            "applicant_name": further_information.get("applicant_name").unwrap_or(&String::new()).to_string(),
            "agent_name": further_information.get("agent_name").unwrap_or(&String::new()).to_string(),
            "agent_company_name": further_information.get("agent_company_name").unwrap_or(&String::new()).to_string(),
            "agent_address": further_information.get("agent_address").unwrap_or(&String::new()).to_string(),
            "environmental_assessment_requested": further_information.get("environmental_assessment_requested").unwrap_or(&String::new()).to_string(),
        },
        "created_at": Some(chrono::Utc::now()),
        "created_by": "6539157ef8be4d62ea02ed6b".to_string(),
        "updated_at": Some(chrono::Utc::now()),
        "updated_by": "6539157ef8be4d62ea02ed6b".to_string(),
        "details_url": url.to_string().replace("=printPreview", "=details"),
        "documents_url": url.to_string().replace("=printPreview", "=documents"),
        "documents": bson::Array::new(),
        "agent_details": {
            "agent_email": agents.get("email").unwrap_or(&String::new()).to_string(),
            "agent_phone": agents.get("mobile_phone").unwrap_or(&String::new()).to_string(),
        },
    };

    Ok(document)
}

/// Extracts documents from a given URL.
///
/// This function performs an HTTP GET request to the specified URL,
/// parses the HTML document to extract a reference ID and a mapping
/// of document types to their URLs. It identifies documents by
/// checking if their names or descriptions contain keywords such as
/// "decision" or "application form".
///
/// # Arguments
///
/// * `client` - An instance of `Client` used to execute HTTP requests.
/// * `url` - A string slice representing the URL of the document to retrieve.
///
/// # Returns
///
/// Returns a `Result` containing a tuple with the reference ID and a
/// `HashMap` of document types to their full URLs if successful, or
/// an error if the operation fails.
pub async fn extract_docs(
    client: &Client,
    url: &str,
) -> Result<(String, HashMap<&'static str, String>)> {
    let html = client
        .get(format!("{}{}", BASE_URL, url))
        .send()
        .await?
        .text()
        .await?;
    let document = Html::parse_document(&html);
    println!("Parsed HTML");
    let reference_id = document
        .select(&Selector::parse(REFERENCE_ID_SELECTOR).expect("Failed to parse selector"))
        .next()
        .ok_or("No reference id found")
        .expect("No reference id found")
        .text()
        .collect::<String>()
        .trim()
        .to_string();

    let docs = document
        .select(&Selector::parse(DOCS_SELECTOR).expect("Failed to parse selector"))
        .map(|e| e.text().collect::<String>())
        .collect::<Vec<_>>();

    let descriptions = document
        .select(&Selector::parse(DESCRIPTION_SELECTOR).expect("Failed to parse selector"))
        .map(|e| e.text().collect::<String>())
        .collect::<Vec<_>>();

    let views = document
        .select(&Selector::parse(DOCS_LINK_SELECTOR).expect("Failed to parse selector"))
        .map(|e| e.value().attr("href").unwrap_or_default().to_string())
        .collect::<Vec<_>>();

    let docs = docs
        .into_iter()
        .zip(views.into_iter())
        .zip(descriptions.into_iter())
        .filter(|((doc, _), desc)| {
            doc.trim().to_lowercase().contains("decision")
                || desc.trim().to_lowercase().contains("decision")
                || doc.trim().to_lowercase().contains("application form")
                || desc.trim().to_lowercase().contains("application form")
        })
        .filter(|((_, view), _)| !view.trim().is_empty())
        .map(|((doc, view), desc)| {
            match doc.trim().to_lowercase().contains("decision")
                || desc.trim().to_lowercase().contains("decision")
            {
                true => ("decision_notice", format!("{}{}", BASE_URL, view)),
                false => ("application_form", format!("{}{}", BASE_URL, view)),
            }
        })
        .collect::<HashMap<&str, String>>();
    Ok((reference_id, docs))
}

/// Downloads and extracts the Leeds planning application documents.
///
/// Downloads the Leeds planning application documents, extracts the reference id and the links to the documents,
/// checks if the reference id already exists in the database, and if not, uploads the documents to S3 and sends
/// the data to the database. If the reference id already exists, it checks if the decision notice exists in the
/// database, and if not, uploads the decision notice to S3 and sends the data to the database.
///
/// # Arguments
///
/// * `url`: The URL of the Leeds planning application page.
/// * `collection`: The MongoDB collection to store the data in.
/// * `s3_client`: The AWS S3 client to use to upload the files.
///
/// # Errors
///
/// This function will return an error if there is a problem downloading the documents, extracting the links,
/// checking if the reference id exists in the database, uploading the documents to S3, or sending the data to the
/// database.
pub async fn process(
    url: &str,
    collection: &mongodb::Collection<Document>,
    s3_client: &aws_sdk_s3::Client,
) -> Result<()> {
    let start_time = std::time::Instant::now();

    let client = Client::builder().cookie_store(true).build()?;
    client.get(BASE_URL).send().await?;

    
    println!("Extracting validated links...");
    let validated_links = extract_links(&client, url, "DC_Validated").await?;
    
    println!("Found {} decided links.", validated_links.len());

    let pb = ProgressBar::new(validated_links.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")?
            .progress_chars("#>-"),
    );

    for link in validated_links {
        thread::sleep(Duration::from_secs(1));
        println!("Processing link: {}", link);
        let document_url = link.replace("=summary", "=documents");
        match extract_docs(&client, &document_url).await {
            Ok((reference_id, docs)) => {
                if let Ok(Some(_)) =
                    check_reference(reference_id.as_str(), COUNTY, collection).await
                {
                    println!("Skipping reference as already present");
                } else {
                    if !docs.contains_key("application_form") {
                        println!(
                            "No application form found for reference id: {}",
                            reference_id
                        );
                    } else {
                        let link = format!("{}{}", BASE_URL, link);
                        let mut document = match get_document(&client, &link).await {
                            Ok(doc) => doc,
                            Err(e) => {
                                println!("Error in getting document: {}", e);
                                pb.inc(1);
                                continue;
                            }
                        };

                        let link = docs.get("application_form").unwrap();
                        let file = match s3upload::upload_file(
                            "application_form",
                            &link,
                            &client,
                            s3_client,
                        )
                        .await
                        {
                            Ok(file) => file,
                            Err(e) => {
                                println!("Error in uploading file: {}", e);
                                pb.inc(1);
                                continue;
                            }
                        };

                        println!("File uploaded to S3");
                        let mut doc = document
                            .get_array("documents")
                            .unwrap_or(&bson::Array::new())
                            .to_owned();
                        doc.push(bson::Bson::Document(file));
                        document.insert("documents", doc);

                        if let Err(e) =
                            mongo::send_data(&reference_id, COUNTY, document, collection, false)
                                .await
                        {
                            println!("Error in sending data: {}", e);
                        }
                    }
                }
            }
            Err(e) => {
                println!("Failed to extract docs for link: {} - {}", link, e);
            }
        }
        pb.inc(1);
    }

    pb.finish();

    println!("Extracting decided links...");
    let decided_links = extract_links(&client, url, "DC_Decided").await?;

    let pb = ProgressBar::new(decided_links.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")?
            .progress_chars("#>-"),
    );

    println!("Found {} decided links.", decided_links.len());
    for link in decided_links {
        thread::sleep(Duration::from_secs(1));
        println!("Processing link: {}", link);
        let document_url = link.replace("=summary", "=documents");
        match extract_docs(&client, &document_url).await {
            Ok((reference_id, docs)) => {
                if let Ok(Some(_)) =
                    check_decision_exisits(reference_id.as_str(), COUNTY, collection).await
                {
                    println!("Skipping as data already present");
                } else if let Ok(Some(document)) =
                    check_reference(reference_id.as_str(), COUNTY, collection).await
                {
                    let mut doc = document
                        .get_array("documents")
                        .unwrap_or(&bson::Array::new())
                        .to_owned();
                    if let Some(decision_link) = docs.get("decision_notice") {
                        let file = match s3upload::upload_file(
                            "decision_notice",
                            &decision_link,
                            &client,
                            s3_client,
                        )
                        .await
                        {
                            Ok(file) => file,
                            Err(e) => {
                                println!("Error in uploading file: {}", e);
                                pb.inc(1);
                                continue;
                            }
                        };
                        println!("File uploaded to S3");
                        doc.push(bson::Bson::Document(file));
                    } else {
                        println!(
                            "No Decision Notice found for reference id: {}",
                            reference_id
                        );
                        pb.inc(1);
                        continue;
                    }

                    let new_document =
                        get_document(&client, &format!("{}{}", BASE_URL, link)).await?;
                    let update = doc! {
                        "$set": {
                            "summary": new_document.get_document("summary").unwrap_or(&bson::Document::new()),
                            "further_information": new_document.get_document("further_information").unwrap_or(&bson::Document::new()),
                            "documents": doc,
                            "agent_details": new_document.get_document("agent_details").unwrap_or(&bson::Document::new()),
                            "updated_at": Some(chrono::Utc::now()),
                            "updated_by": "6539157ef8be4d62ea02ed6b".to_string(),
                        },
                    };
                    if let Err(e) =
                        mongo::send_data(&reference_id, COUNTY, update, collection, true).await
                    {
                        println!("Error in sending data: {}", e);
                    }
                } else {
                    let link = format!("{}{}", BASE_URL, link);
                    let mut document = match get_document(&client, &link).await {
                        Ok(doc) => doc,
                        Err(e) => {
                            println!("Error in getting document: {}", e);
                            pb.inc(1);
                            continue;
                        }
                    };
                    let mut doc = bson::Array::new();
                    for (k, v) in docs {
                        let file = match s3upload::upload_file(k, &v, &client, s3_client).await {
                            Ok(file) => file,
                            Err(e) => {
                                println!("Error in uploading file: {}", e);
                                pb.inc(1);
                                continue;
                            }
                        };
                        println!("File uploaded to S3");
                        doc.push(bson::Bson::Document(file));
                    }
                    document.insert("documents", doc);

                    if let Err(e) =
                        mongo::send_data(&reference_id, COUNTY, document, collection, false).await
                    {
                        println!("Error in sending data: {}", e);
                    }
                }
            }
            Err(e) => {
                println!("Failed to extract docs for link: {} - {}", link, e);
            }
        }
        pb.inc(1);
    }

    pb.finish();

    let end_time = std::time::Instant::now();
    let duration = end_time - start_time;
    println!("Downloaded and extracted in {} seconds", duration.as_secs());

    Ok(())
}
