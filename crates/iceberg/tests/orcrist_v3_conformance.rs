// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Orcrist fork conformance over an Iceberg v3 table written by an
//! independent writer (Apache Spark with Iceberg Java, see
//! `.github/workflows/orcrist-v3-conformance.yml`). The tests skip unless
//! `ORCRIST_V3_METADATA` names the table's metadata file. The seeding test
//! temporarily renames the table's manifest files and restores them.

use futures::TryStreamExt;
use iceberg::TableIdent;
use iceberg::io::FileIO;
use iceberg::spec::FormatVersion;
use iceberg::table::{StaticTable, Table};

async fn load(metadata: &str) -> Table {
    StaticTable::from_metadata_file(
        metadata,
        TableIdent::from_strs(["db", "t"]).unwrap(),
        FileIO::new_with_fs(),
    )
    .await
    .unwrap()
    .into_table()
}

async fn plan(table: &Table) -> Vec<String> {
    let scan = table.scan().build().unwrap();
    let mut tasks: Vec<String> = scan
        .plan_files()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .into_iter()
        .map(|task| {
            let mut deletes: Vec<String> = task
                .deletes()
                .iter()
                .map(|delete| {
                    format!(
                        "{}:{:?}:{:?}",
                        delete.file_path, delete.file_type, delete.file_format
                    )
                })
                .collect();
            deletes.sort();
            format!(
                "{}|{}|{}|{:?}",
                task.data_file_path(),
                task.start(),
                task.length(),
                deletes
            )
        })
        .collect();
    tasks.sort();
    tasks
}

fn env() -> Option<String> {
    std::env::var("ORCRIST_V3_METADATA").ok()
}

#[tokio::test]
async fn writer_produced_v3_table_plans() {
    let Some(metadata) = env() else {
        eprintln!("ORCRIST_V3_METADATA unset; skipping");
        return;
    };
    let table = load(&metadata).await;
    assert_eq!(table.metadata().format_version(), FormatVersion::V3);
    let tasks = plan(&table).await;
    assert!(!tasks.is_empty());
    // The Spark script deletes with merge-on-read, so v3 deletion vectors exist.
    assert!(
        tasks.iter().any(|task| task.contains(".puffin")),
        "expected a deletion vector in {tasks:?}"
    );
}

/// Seeding every manifest the caller parsed makes planning independent of the
/// manifest files, and gives the same plan as an unseeded table.
#[tokio::test]
async fn seeded_manifests_plan_without_rereading_files() {
    let Some(metadata) = env() else {
        eprintln!("ORCRIST_V3_METADATA unset; skipping");
        return;
    };
    let baseline = plan(&load(&metadata).await).await;

    let table = load(&metadata).await;
    let snapshot = table.metadata().current_snapshot().unwrap().clone();
    let list = table.manifest_list_reader(&snapshot).load().await.unwrap();
    assert!(!list.entries().is_empty());
    let mut moved = Vec::new();
    for manifest_file in list.entries() {
        let manifest = table.manifest_reader().read(manifest_file).await.unwrap();
        assert!(
            table
                .seed_manifest_cache(manifest_file, std::sync::Arc::new(manifest))
                .await
        );
        let path = manifest_file
            .manifest_path
            .strip_prefix("file://")
            .or_else(|| manifest_file.manifest_path.strip_prefix("file:"))
            .unwrap_or(&manifest_file.manifest_path)
            .to_string();
        let hidden = format!("{path}.hidden");
        std::fs::rename(&path, &hidden).unwrap();
        moved.push((path, hidden));
    }
    let seeded = plan(&table).await;
    for (path, hidden) in moved {
        std::fs::rename(hidden, path).unwrap();
    }
    assert_eq!(baseline, seeded);
}
