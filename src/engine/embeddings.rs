use anyhow::Result;
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

#[derive(Debug, Clone)]
pub struct Document {
    pub id: String,
    pub content: String,
    pub path: String,
    pub embedding: Option<Vec<f32>>,
}

pub async fn embed_text(text: &str) -> Result<Vec<f32>> {
    let mut embedding = Vec::with_capacity(128);
    for i in 0..128 {
        let byte = *text.as_bytes().get(i).unwrap_or(&0) as f32;
        embedding.push(((byte / 255.0) * (1.0 + (i as f32 / 128.0))).min(1.0));
    }
    Ok(embedding)
}

pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() {
        return 0.0;
    }

    let dot_product: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();

    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }

    dot_product / (norm_a * norm_b)
}

pub async fn scan_local_files() -> Result<Vec<Document>> {
    let mut documents = Vec::new();
    let extensions = ["rs", "md", "toml", "yaml", "json"];

    for entry in walkdir::WalkDir::new(".")
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
    {
        let path = entry.path();
        if let Some(ext) = path.extension() {
            if extensions.contains(&ext.to_str().unwrap_or("")) {
                if let Ok(content) = std::fs::read_to_string(path) {
                    for (i, chunk) in content.chars().collect::<Vec<_>>().chunks(2000).enumerate() {
                        let chunk_content: String = chunk.iter().collect();
                        if chunk_content.trim().len() > 50 {
                            documents.push(Document {
                                id: format!("{}#{}", path.display(), i),
                                content: chunk_content,
                                path: path.to_string_lossy().to_string(),
                                embedding: None,
                            });
                        }
                    }
                }
            }
        }
    }

    Ok(documents)
}

pub async fn build_embeddings_index() -> Result<Vec<Document>> {
    let mut documents = scan_local_files().await?;

    for doc in &mut documents {
        match embed_text(&doc.content).await {
            Ok(embedding) => doc.embedding = Some(embedding),
            Err(e) => {
                tracing::warn!("Failed to embed {}: {}", doc.id, e);
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }

    Ok(documents)
}
