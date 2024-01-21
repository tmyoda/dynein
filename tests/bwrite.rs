/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

pub mod util;

use assert_cmd::prelude::*; // Add methods on commands
use base64::{engine::general_purpose, Engine as _};
use predicates::prelude::*; // Used for writing assertions
use serde_json::Value;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use tempfile::Builder;

#[tokio::test]
async fn test_batch_write_json_put() -> Result<(), Box<dyn std::error::Error>> {
    let mut tm = util::setup().await?;
    let table_name = tm.create_temporary_table("pk", None).await?;

    let tmpdir = Builder::new().tempdir()?; // defining stand alone variable here as tempfile::tempdir creates directory and deletes it when the destructor is run.
    let batch_input_file_path = create_test_json_file(
        "tests/resources/test_batch_write_put.json",
        vec![&table_name],
        &tmpdir,
    );

    let mut c = tm.command()?;
    c.args(&[
        "--region",
        "local",
        "bwrite",
        "--input",
        &batch_input_file_path,
    ])
    .output()?;

    let mut c = tm.command()?;
    let scan_cmd = c.args(&[
        "--region",
        "local",
        "--table",
        &table_name,
        "scan",
        "-o",
        "raw",
    ]);
    let output = scan_cmd.output()?.stdout;
    // Catch an error if key does not exist
    let mut output_json: Value =
        serde_json::from_str::<serde_json::Value>(&String::from_utf8(output)?)?
            .get(0)
            .unwrap()
            .clone();

    let org_json_string = std::fs::read_to_string("tests/resources/test_batch_write_put.json")?;
    // Catch an error if key does not exist
    let mut org_json = serde_json::from_str::<serde_json::Value>(&org_json_string)?
        .get("__TABLE_NAME__1")
        .unwrap()
        .get(0)
        .unwrap()
        .get("PutRequest")
        .unwrap()
        .get("Item")
        .unwrap()
        .clone();

    // The order of the values within a set is not preserved, so I will sort it.
    sort_json_array(&mut output_json);
    sort_json_array(&mut org_json);

    assert_eq!(output_json, org_json);

    Ok(())
}

#[tokio::test]
async fn test_batch_write_json_delete() -> Result<(), Box<dyn std::error::Error>> {
    let mut tm = util::setup().await?;
    let table_name = tm
        .create_temporary_table_with_items(
            "pk",
            None,
            vec![util::TemporaryItem::new(
                "ichi",
                None,
                Some(r#"{"null-field": null}"#),
            )],
        )
        .await?;

    let mut tm = util::setup().await?;
    let table_name_sk = tm
        .create_temporary_table_with_items(
            "pk",
            Some("sk"),
            vec![util::TemporaryItem::new(
                "ichi",
                Some("sortkey"),
                Some(r#"{"null-field": null}"#),
            )],
        )
        .await?;

    let tmpdir = Builder::new().tempdir()?; // defining stand alone variable here as tempfile::tempdir creates directory and deletes it when the destructor is run.
    let batch_input_file_path = create_test_json_file(
        "tests/resources/test_batch_write_delete.json",
        vec![&table_name, &table_name_sk],
        &tmpdir,
    );

    let mut c = tm.command()?;
    c.args(&[
        "--region",
        "local",
        "bwrite",
        "--input",
        &batch_input_file_path,
    ])
    .output()?;

    let mut c = tm.command()?;
    let scan_cmd = c.args(&[
        "--region",
        "local",
        "--table",
        &table_name,
        "scan",
        "-o",
        "raw",
    ]);
    scan_cmd.assert().success().stdout("[]\n");

    let mut c = tm.command()?;
    let scan_cmd = c.args(&[
        "--region",
        "local",
        "--table",
        &table_name_sk,
        "scan",
        "-o",
        "raw",
    ]);
    scan_cmd.assert().success().stdout("[]\n");

    Ok(())
}

#[tokio::test]
async fn test_batch_write_json_put_delete() -> Result<(), Box<dyn std::error::Error>> {
    let mut tm = util::setup().await?;
    let table_name = tm
        .create_temporary_table_with_items(
            "pk",
            None,
            vec![util::TemporaryItem::new(
                "ichi",
                None,
                Some(r#"{"null-field": null}"#),
            )],
        )
        .await?;

    let tmpdir = Builder::new().tempdir()?; // defining stand alone variable here as tempfile::tempdir creates directory and deletes it when the destructor is run.
    let batch_input_file_path = create_test_json_file(
        "tests/resources/test_batch_write_put_delete.json",
        vec![&table_name],
        &tmpdir,
    );

    let mut c = tm.command()?;
    c.args(&[
        "--region",
        "local",
        "bwrite",
        "--input",
        &batch_input_file_path,
    ])
    .output()?;

    let mut c = tm.command()?;
    let scan_cmd = c.args(&[
        "--region",
        "local",
        "--table",
        &table_name,
        "scan",
        "-o",
        "json",
    ]);
    scan_cmd.assert().success().stdout(
        predicate::str::is_match(r#"pk": "ni""#)?.and(predicate::str::is_match(r#"pk": "san""#)?),
    );

    Ok(())
}

#[tokio::test]
async fn test_batch_write_json_put_delete_multiple_tables() -> Result<(), Box<dyn std::error::Error>>
{
    let mut tm = util::setup().await?;
    let table_name = tm
        .create_temporary_table_with_items(
            "pk",
            None,
            vec![util::TemporaryItem::new(
                "ichi",
                None,
                Some(r#"{"null-field": null}"#),
            )],
        )
        .await?;

    let table_name2 = tm
        .create_temporary_table_with_items(
            "pk",
            None,
            vec![util::TemporaryItem::new(
                "ichi",
                None,
                Some(r#"{"null-field": null}"#),
            )],
        )
        .await?;

    let tmpdir = Builder::new().tempdir()?; // defining stand alone variable here as tempfile::tempdir creates directory and deletes it when the destructor is run.
    let batch_input_file_path = create_test_json_file(
        "tests/resources/test_batch_write_put_delete_multiple_tables.json",
        vec![&table_name, &table_name2],
        &tmpdir,
    );

    let mut c = tm.command()?;
    c.args(&[
        "--region",
        "local",
        "bwrite",
        "--input",
        &batch_input_file_path,
    ])
    .output()?;

    let mut c = tm.command()?;
    let scan_cmd = c.args(&[
        "--region",
        "local",
        "--table",
        &table_name,
        "scan",
        "-o",
        "json",
    ]);
    scan_cmd.assert().success().stdout(
        predicate::str::is_match(r#"pk": "ni""#)?.and(predicate::str::is_match(r#"pk": "san""#)?),
    );

    let mut c = tm.command()?;
    let scan_cmd = c.args(&[
        "--region",
        "local",
        "--table",
        &table_name2,
        "scan",
        "-o",
        "json",
    ]);
    scan_cmd.assert().success().stdout(
        predicate::str::is_match(r#"pk": "ni""#)?.and(predicate::str::is_match(r#"pk": "san""#)?),
    );

    Ok(())
}

#[tokio::test]
async fn test_batch_write_put() -> Result<(), Box<dyn std::error::Error>> {
    let mut tm = util::setup().await?;
    let table_name = tm.create_temporary_table("pk", None).await?;

    let mut c = tm.command()?;
    c.args(&[
        "--region",
        "local",
        "--table",
        &table_name,
        "bwrite",
        "--put",
        r#"{"pk": "11",
        "null-field": null,
        "list-field": [1, 2, 3, "str"],
        "map-field": {"l0": <<1, 2>>, "l1": <<"str1", "str2">>, "l2": true},
        "binary-field": b"\x00",
        "binary-set-field": <<b"\x01", b"\x02">>}"#,
    ])
    .output()?;

    let mut c = tm.command()?;
    let get_cmd = c.args(&[
        "--region",
        "local",
        "--table",
        &table_name,
        "get",
        "11",
        "-o",
        "raw",
    ]);
    let output = get_cmd.output()?.stdout;
    let data: Value = serde_json::from_str(&String::from_utf8(output)?)?;
    assert_eq!(data["pk"]["S"], "11");
    assert_eq!(data["null-field"]["NULL"], true);
    assert_eq!(data["list-field"]["L"][0]["N"], "1");
    assert_eq!(data["list-field"]["L"][1]["N"], "2");
    assert_eq!(data["list-field"]["L"][2]["N"], "3");
    assert_eq!(data["list-field"]["L"][3]["S"], "str");
    assert_eq!(data["map-field"]["M"]["l0"]["NS"][0], "1");
    assert_eq!(data["map-field"]["M"]["l0"]["NS"][1], "2");
    assert_eq!(data["map-field"]["M"]["l1"]["SS"][0], "str1");
    assert_eq!(data["map-field"]["M"]["l1"]["SS"][1], "str2");
    assert_eq!(data["map-field"]["M"]["l2"]["BOOL"], true);
    assert_eq!(
        data["binary-field"]["B"],
        general_purpose::STANDARD.encode(b"\x00")
    );
    assert_eq!(
        data["binary-set-field"]["BS"][0],
        general_purpose::STANDARD.encode(b"\x01")
    );
    assert_eq!(
        data["binary-set-field"]["BS"][1],
        general_purpose::STANDARD.encode(b"\x02")
    );

    Ok(())
}

#[tokio::test]
async fn test_batch_write_put_sk() -> Result<(), Box<dyn std::error::Error>> {
    let mut tm = util::setup().await?;
    let table_name = tm.create_temporary_table("pk", Some("sk")).await?;

    let mut c = tm.command()?;
    c.args(&[
        "--region",
        "local",
        "--table",
        &table_name,
        "bwrite",
        "--put",
        r#"{"pk": "11", "sk": "111"}"#,
    ])
    .output()?;

    let mut c = tm.command()?;
    let get_cmd = c.args(&[
        "--region",
        "local",
        "--table",
        &table_name,
        "get",
        "11",
        "111",
        "-o",
        "json",
    ]);
    get_cmd.assert().success().stdout(
        predicate::str::is_match(r#"pk": "11""#)?.and(predicate::str::is_match(r#"sk": "111""#)?),
    );

    Ok(())
}

#[tokio::test]
async fn test_batch_write_del() -> Result<(), Box<dyn std::error::Error>> {
    let mut tm = util::setup().await?;
    let table_name = tm
        .create_temporary_table_with_items(
            "pk",
            None,
            vec![util::TemporaryItem::new(
                "11",
                None,
                Some(r#"{"null-field": null}"#),
            )],
        )
        .await?;

    let mut c = tm.command()?;
    c.args(&[
        "--region",
        "local",
        "--table",
        &table_name,
        "bwrite",
        "--del",
        r#"{"pk": "11"}"#,
    ])
    .output()?;

    let mut c = tm.command()?;
    let scan_cmd = c.args(&[
        "--region",
        "local",
        "--table",
        &table_name,
        "scan",
        "-o",
        "raw",
    ]);
    scan_cmd.assert().success().stdout("[]\n");

    Ok(())
}

#[tokio::test]
async fn test_batch_write_del_sk() -> Result<(), Box<dyn std::error::Error>> {
    let mut tm = util::setup().await?;
    let table_name = tm
        .create_temporary_table_with_items(
            "pk",
            Some("sk"),
            vec![util::TemporaryItem::new(
                "11",
                Some("111"),
                Some(r#"{"null-field": null}"#),
            )],
        )
        .await?;

    let mut c = tm.command()?;
    c.args(&[
        "--region",
        "local",
        "--table",
        &table_name,
        "bwrite",
        "--del",
        r#"{"pk": "11", "sk": "111"}"#,
    ])
    .output()?;

    let mut c = tm.command()?;
    let scan_cmd = c.args(&[
        "--region",
        "local",
        "--table",
        &table_name,
        "scan",
        "-o",
        "raw",
    ]);
    scan_cmd.assert().success().stdout("[]\n");

    Ok(())
}

#[tokio::test]
async fn test_batch_write_all_options() -> Result<(), Box<dyn std::error::Error>> {
    let mut tm = util::setup().await?;
    let table_name = tm
        .create_temporary_table_with_items(
            "pk",
            None,
            vec![
                util::TemporaryItem::new("11", None, Some(r#"{"null-field": null}"#)),
                util::TemporaryItem::new("ichi", None, Some(r#"{"null-field": null}"#)),
            ],
        )
        .await?;

    let tmpdir = Builder::new().tempdir()?; // defining stand alone variable here as tempfile::tempdir creates directory and deletes it when the destructor is run.
    let batch_input_file_path = create_test_json_file(
        "tests/resources/test_batch_write_put_delete.json",
        vec![&table_name],
        &tmpdir,
    );
    let mut c = tm.command()?;
    c.args(&[
        "--region",
        "local",
        "--table",
        &table_name,
        "bwrite",
        "--del",
        r#"{"pk": "11"}"#,
        "--input",
        &batch_input_file_path,
        "--put",
        r#"{"pk": "12", "null-field": null}"#,
    ])
    .output()?;

    let mut c = tm.command()?;
    let scan_cmd = c.args(&[
        "--region",
        "local",
        "--table",
        &table_name,
        "scan",
        "-o",
        "json",
    ]);
    let output = scan_cmd.output()?.stdout;
    let output_str = String::from_utf8(output)?;

    // Check if the first item has been deleted
    assert_eq!(
        false,
        predicate::str::is_match(r#""pk": "11""#)?.eval(&output_str)
    );
    assert_eq!(
        false,
        predicate::str::is_match(r#""pk": "ichi""#)?.eval(&output_str)
    );
    // Check if the json item put exists
    assert!(predicate::str::is_match(r#"pk": "12""#)?.eval(&output_str));
    // Check if the command inputs exists
    assert!(predicate::str::is_match(r#""pk": "ni""#)?.eval(&output_str));
    assert!(predicate::str::is_match(r#"pk": "san""#)?.eval(&output_str));

    Ok(())
}

fn create_test_json_file(
    json_path: &str,
    table_names: Vec<&String>,
    tmpdir: &tempfile::TempDir,
) -> String {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push(json_path);

    let mut test_json_content = std::fs::read_to_string(&path).unwrap();
    let file_name = path.file_name().unwrap();

    let batch_input_file_path = tmpdir.path().join(file_name);
    let mut f = File::create(&batch_input_file_path).unwrap();
    for (i, tbn) in table_names.iter().enumerate() {
        test_json_content = test_json_content.replace(&format!("__TABLE_NAME__{}", i + 1), tbn);
    }

    f.write_all(test_json_content.as_bytes()).unwrap();

    batch_input_file_path.to_str().unwrap().to_owned()
}

fn sort_json_array(value: &mut Value) {
    match value {
        Value::Array(arr) => {
            arr.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
        }
        Value::Object(obj) => {
            for v in obj.values_mut() {
                sort_json_array(v);
            }
        }
        _ => {}
    }
}
