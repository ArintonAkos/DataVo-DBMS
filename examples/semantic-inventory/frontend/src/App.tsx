import { useEffect, useMemo, useState } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";

type CatalogRow = {
  id: number;
  name: string;
  category: string;
  price: number;
  quantity: number;
  location: string;
};

type TopSellerRow = {
  itemId: number;
  name: string;
  unitsSold: number;
  revenue: number;
};

type SemanticRow = {
  itemId: number;
  name: string;
  category: string;
  price: number;
  quantity: number;
  score: number;
};

const API = (import.meta.env.VITE_API_BASE_URL as string | undefined) ?? "http://127.0.0.1:5088";

export default function App() {
  const [catalog, setCatalog] = useState<CatalogRow[]>([]);
  const [topSellers, setTopSellers] = useState<TopSellerRow[]>([]);
  const [semanticRows, setSemanticRows] = useState<SemanticRow[]>([]);
  const [category, setCategory] = useState("");
  const [query, setQuery] = useState("ergonomic desk furniture for office");
  const [topK, setTopK] = useState(10);
  const [orderItemId, setOrderItemId] = useState(1);
  const [orderQty, setOrderQty] = useState(1);
  const [orderMessage, setOrderMessage] = useState("");

  const totalStock = useMemo(() => catalog.reduce((acc, row) => acc + row.quantity, 0), [catalog]);

  async function loadCatalog(filter?: string) {
    const suffix = filter ? `?category=${encodeURIComponent(filter)}` : "";
    const res = await fetch(`${API}/api/catalog${suffix}`);
    setCatalog(await res.json());
  }

  async function loadTopSellers() {
    const res = await fetch(`${API}/api/top-sellers?take=12`);
    setTopSellers(await res.json());
  }

  async function runSearch() {
    const res = await fetch(`${API}/api/search`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query, topK })
    });

    if (!res.ok) {
      setSemanticRows([]);
      return;
    }

    setSemanticRows(await res.json());
  }

  async function placeOrder() {
    const res = await fetch(`${API}/api/order`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ itemId: orderItemId, qty: orderQty })
    });

    const payload = await res.json();
    setOrderMessage(payload.message ?? "Order response received.");
    await Promise.all([loadCatalog(category || undefined), loadTopSellers()]);
  }

  useEffect(() => {
    void Promise.all([loadCatalog(), loadTopSellers()]);
  }, []);

  return (
    <div className="mx-auto max-w-7xl p-6 md:p-10">
      <header className="mb-6 rounded-xl border border-border bg-white/90 p-6 shadow-sm backdrop-blur">
        <h1 className="text-3xl font-bold text-foreground">Semantic Inventory Console</h1>
        <p className="mt-2 text-sm text-neutral-600">
          React + shadcn-style UI on top of DataVo + Ollama embeddings. Seeded with thousands of items.
        </p>
      </header>

      <section className="mb-6 grid gap-4 md:grid-cols-3">
        <Card>
          <CardTitle>Catalog Items</CardTitle>
          <p className="mt-2 text-3xl font-semibold">{catalog.length.toLocaleString()}</p>
        </Card>
        <Card>
          <CardTitle>Total Stock</CardTitle>
          <p className="mt-2 text-3xl font-semibold">{totalStock.toLocaleString()}</p>
        </Card>
        <Card>
          <CardTitle>Top Seller</CardTitle>
          <p className="mt-2 text-2xl font-semibold">{topSellers[0]?.name ?? "-"}</p>
        </Card>
      </section>

      <section className="grid gap-6 lg:grid-cols-2">
        <Card>
          <CardTitle>Catalog</CardTitle>
          <div className="mt-4 flex gap-2">
            <Input value={category} onChange={(e) => setCategory(e.target.value)} placeholder="Filter category" />
            <Button onClick={() => void loadCatalog(category || undefined)}>Apply</Button>
            <Button variant="outline" onClick={() => { setCategory(""); void loadCatalog(); }}>Clear</Button>
          </div>
          <div className="mt-4 max-h-96 overflow-auto rounded border border-border">
            <table className="w-full text-sm">
              <thead className="sticky top-0 bg-muted">
                <tr>
                  <th className="p-2 text-left">Name</th>
                  <th className="p-2 text-left">Category</th>
                  <th className="p-2 text-left">Price</th>
                  <th className="p-2 text-left">Qty</th>
                </tr>
              </thead>
              <tbody>
                {catalog.slice(0, 500).map((row) => (
                  <tr key={row.id} className="border-t border-border">
                    <td className="p-2">{row.name}</td>
                    <td className="p-2">{row.category}</td>
                    <td className="p-2">${row.price.toFixed(2)}</td>
                    <td className="p-2">{row.quantity}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>

        <Card>
          <CardTitle>Semantic Search</CardTitle>
          <div className="mt-4 flex gap-2">
            <Input value={query} onChange={(e) => setQuery(e.target.value)} placeholder="Describe what you want" />
            <Input
              type="number"
              min={1}
              max={50}
              value={topK}
              onChange={(e) => setTopK(Number(e.target.value))}
              className="max-w-24"
            />
            <Button onClick={() => void runSearch()}>Search</Button>
          </div>
          <div className="mt-4 max-h-64 overflow-auto rounded border border-border">
            <table className="w-full text-sm">
              <thead className="sticky top-0 bg-muted">
                <tr>
                  <th className="p-2 text-left">Item</th>
                  <th className="p-2 text-left">Category</th>
                  <th className="p-2 text-left">Score</th>
                </tr>
              </thead>
              <tbody>
                {semanticRows.map((row) => (
                  <tr key={row.itemId} className="border-t border-border">
                    <td className="p-2">{row.name}</td>
                    <td className="p-2">{row.category}</td>
                    <td className="p-2">{row.score.toFixed(4)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>

        <Card>
          <CardTitle>Top Sellers</CardTitle>
          <div className="mt-4 max-h-64 overflow-auto rounded border border-border">
            <table className="w-full text-sm">
              <thead className="sticky top-0 bg-muted">
                <tr>
                  <th className="p-2 text-left">Name</th>
                  <th className="p-2 text-left">Units</th>
                  <th className="p-2 text-left">Revenue</th>
                </tr>
              </thead>
              <tbody>
                {topSellers.map((row) => (
                  <tr key={row.itemId} className="border-t border-border">
                    <td className="p-2">{row.name}</td>
                    <td className="p-2">{row.unitsSold}</td>
                    <td className="p-2">${row.revenue.toFixed(2)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>

        <Card>
          <CardTitle>Place Order</CardTitle>
          <div className="mt-4 grid gap-2 sm:grid-cols-3">
            <Input type="number" min={1} value={orderItemId} onChange={(e) => setOrderItemId(Number(e.target.value))} />
            <Input type="number" min={1} value={orderQty} onChange={(e) => setOrderQty(Number(e.target.value))} />
            <Button onClick={() => void placeOrder()}>Submit</Button>
          </div>
          {orderMessage && <p className="mt-3 text-sm text-neutral-700">{orderMessage}</p>}
        </Card>
      </section>
    </div>
  );
}
