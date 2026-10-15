"""Read-only retrieval over the pipeline's existing stores."""
import lancedb
from qdrant_client import QdrantClient, models


def rrf(rankings, k):
    scores, rows = {}, {}
    for ranking in rankings:
        for rank, row in enumerate(ranking, 1):
            key = row['id']
            rows[key] = row
            scores[key] = scores.get(key, 0) + 1 / (60 + rank)
    return [rows[key] for key in sorted(scores, key=lambda x: (-scores[x], x))[:k]]


class Retriever:
    def __init__(self, work, name, backends, qdrant_url=None):
        self.table = self.qdrant = self.sparse = None
        self.name = name
        if 'lancedb' in backends:
            self.table = lancedb.connect(str(work / 'lancedb')).open_table(name)
            self.table.checkout(self.table.version)
        if 'qdrant' in backends:
            import os
            self.qdrant = (QdrantClient(url=qdrant_url, api_key=os.getenv('QDRANT_API_KEY'), timeout=120)
                           if qdrant_url else QdrantClient(path=str(work / 'qdrant')))
            from fastembed import SparseTextEmbedding
            self.sparse = SparseTextEmbedding(model_name='Qdrant/bm25')

    def state(self):
        result = {}
        if self.table is not None:
            result['lancedb'] = {'version': self.table.version, 'count': self.table.count_rows()}
        if self.qdrant is not None:
            result['qdrant'] = {'count': self.qdrant.count(self.name, exact=True).count}
        return result

    def search(self, backend, method, text, vector, k):
        if method == 'hybrid':
            return rrf([self.search(backend, m, text, vector, k * 3) for m in ('bm25', 'vector')], k)
        if backend == 'lancedb':
            query = (self.table.search(text, query_type='fts') if method == 'bm25'
                     else self.table.search(vector, vector_column_name='vector').distance_type('cosine'))
            rows = query.limit(k).to_list()
        else:
            if method == 'bm25':
                sv = next(iter(self.sparse.query_embed(text)))
                query = models.SparseVector(indices=sv.indices.tolist(), values=sv.values.tolist())
            else:
                query = vector
            rows = [p.payload for p in self.qdrant.query_points(self.name, query=query,
                    using='bm25' if method == 'bm25' else 'dense', limit=k, with_payload=True).points]
        return [{key: value for key, value in r.items() if key not in ('vector', '_distance', '_score')} for r in rows]

    def coverage(self, accessions):
        """Evaluation-only corpus availability; never used to filter retrieval."""
        result = {}
        for accession in sorted(set(accessions)):
            counts = {}
            if self.table is not None:
                escaped = accession.replace("'", "''")
                counts['lancedb'] = self.table.count_rows(f"accession = '{escaped}'")
            if self.qdrant is not None:
                counts['qdrant'] = self.qdrant.count(self.name, exact=True,
                    count_filter=models.Filter(must=[models.FieldCondition(key='accession',
                        match=models.MatchValue(value=accession))])).count
            result[accession] = counts
        return result

    def close(self):
        if self.qdrant is not None:
            self.qdrant.close()
