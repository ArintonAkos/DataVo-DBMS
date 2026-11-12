using System.Diagnostics;
using Research.Benchmark.Runners.VectorSearch;

int dims = 1536, k = 10, nq = 100;
foreach (int n in new[] { 1_000, 10_000, 50_000 })
foreach (bool clustered in new[] { false, true })
{
    var rng = new Random(7);
    float[] Unit(float[] v){ double s=0; foreach(var x in v) s+=x*(double)x; float nrm=(float)Math.Sqrt(s); for(int i=0;i<v.Length;i++) v[i]/=nrm; return v; }
    // Uniform: same generator as the benchmark. Clustered: 50 centroids + gaussian-ish noise.
    var centers = Enumerable.Range(0, 50).Select(_ => Unit(Enumerable.Range(0, dims).Select(_ => (float)(rng.NextDouble()*2-1)).ToArray())).ToArray();
    float[] Make() {
        if (!clustered) return Unit(Enumerable.Range(0, dims).Select(_ => (float)(rng.NextDouble()*2-1)).ToArray());
        var c = centers[rng.Next(centers.Length)];
        return Unit(c.Select(x => x + (float)((rng.NextDouble()*2-1) * 0.02)).ToArray());
    }
    var data = Enumerable.Range(0, n).Select(_ => Make()).ToArray();
    var held = Enumerable.Range(0, nq).Select(_ => Make()).ToArray();   // held-out queries

    var flat = new DataVoVectorSearchEngine("FLAT", "Flat", n);
    var hnsw = new DataVoVectorSearchEngine("HNSW", "HNSW", n);
    double[] build = new double[2]; int e = 0;
    foreach (var eng in new[] { flat, hnsw })
    {
        eng.Initialize(dims);
        var sw = Stopwatch.StartNew();
        eng.BeginBatch();
        for (int i = 0; i < n; i++) eng.Insert(i, (float[])data[i].Clone());
        eng.CompleteBatch();
        build[e++] = sw.Elapsed.TotalMilliseconds;
    }
    foreach (var q in held.Take(10)) { flat.Search(q, k); hnsw.Search(q, k); } // warm-up
    double tf=0, th=0, recall=0;
    foreach (var q in held)
    {
        long t0 = Stopwatch.GetTimestamp();
        var f = flat.Search(q, k);
        long t1 = Stopwatch.GetTimestamp();
        var h = hnsw.Search(q, k);
        long t2 = Stopwatch.GetTimestamp();
        tf += Stopwatch.GetElapsedTime(t0, t1).TotalMilliseconds;
        th += Stopwatch.GetElapsedTime(t1, t2).TotalMilliseconds;
        recall += f.Intersect(h).Count() / (double)k;
    }
    Console.WriteLine($"N={n,6} {(clustered ? "clustered" : "uniform  ")} build flat {build[0],9:F0} ms hnsw {build[1],9:F0} ms | query mean flat {tf/nq,7:F3} ms hnsw {th/nq,7:F3} ms | HNSW recall@{k} vs exact {recall/nq:P1}");
    flat.Dispose(); hnsw.Dispose();
}
