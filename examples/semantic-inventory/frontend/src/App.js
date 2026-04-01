import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useEffect, useMemo, useState } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
const API = import.meta.env.VITE_API_BASE_URL ?? "http://127.0.0.1:5088";
export default function App() {
    const [catalog, setCatalog] = useState([]);
    const [topSellers, setTopSellers] = useState([]);
    const [semanticRows, setSemanticRows] = useState([]);
    const [category, setCategory] = useState("");
    const [query, setQuery] = useState("ergonomic desk furniture for office");
    const [topK, setTopK] = useState(10);
    const [orderItemId, setOrderItemId] = useState(1);
    const [orderQty, setOrderQty] = useState(1);
    const [orderMessage, setOrderMessage] = useState("");
    const totalStock = useMemo(() => catalog.reduce((acc, row) => acc + row.quantity, 0), [catalog]);
    async function loadCatalog(filter) {
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
    return (_jsxs("div", { className: "mx-auto max-w-7xl p-6 md:p-10", children: [_jsxs("header", { className: "mb-6 rounded-xl border border-border bg-white/90 p-6 shadow-sm backdrop-blur", children: [_jsx("h1", { className: "text-3xl font-bold text-foreground", children: "Semantic Inventory Console" }), _jsx("p", { className: "mt-2 text-sm text-neutral-600", children: "React + shadcn-style UI on top of DataVo + Ollama embeddings. Seeded with thousands of items." })] }), _jsxs("section", { className: "mb-6 grid gap-4 md:grid-cols-3", children: [_jsxs(Card, { children: [_jsx(CardTitle, { children: "Catalog Items" }), _jsx("p", { className: "mt-2 text-3xl font-semibold", children: catalog.length.toLocaleString() })] }), _jsxs(Card, { children: [_jsx(CardTitle, { children: "Total Stock" }), _jsx("p", { className: "mt-2 text-3xl font-semibold", children: totalStock.toLocaleString() })] }), _jsxs(Card, { children: [_jsx(CardTitle, { children: "Top Seller" }), _jsx("p", { className: "mt-2 text-2xl font-semibold", children: topSellers[0]?.name ?? "-" })] })] }), _jsxs("section", { className: "grid gap-6 lg:grid-cols-2", children: [_jsxs(Card, { children: [_jsx(CardTitle, { children: "Catalog" }), _jsxs("div", { className: "mt-4 flex gap-2", children: [_jsx(Input, { value: category, onChange: (e) => setCategory(e.target.value), placeholder: "Filter category" }), _jsx(Button, { onClick: () => void loadCatalog(category || undefined), children: "Apply" }), _jsx(Button, { variant: "outline", onClick: () => { setCategory(""); void loadCatalog(); }, children: "Clear" })] }), _jsx("div", { className: "mt-4 max-h-96 overflow-auto rounded border border-border", children: _jsxs("table", { className: "w-full text-sm", children: [_jsx("thead", { className: "sticky top-0 bg-muted", children: _jsxs("tr", { children: [_jsx("th", { className: "p-2 text-left", children: "Name" }), _jsx("th", { className: "p-2 text-left", children: "Category" }), _jsx("th", { className: "p-2 text-left", children: "Price" }), _jsx("th", { className: "p-2 text-left", children: "Qty" })] }) }), _jsx("tbody", { children: catalog.slice(0, 500).map((row) => (_jsxs("tr", { className: "border-t border-border", children: [_jsx("td", { className: "p-2", children: row.name }), _jsx("td", { className: "p-2", children: row.category }), _jsxs("td", { className: "p-2", children: ["$", row.price.toFixed(2)] }), _jsx("td", { className: "p-2", children: row.quantity })] }, row.id))) })] }) })] }), _jsxs(Card, { children: [_jsx(CardTitle, { children: "Semantic Search" }), _jsxs("div", { className: "mt-4 flex gap-2", children: [_jsx(Input, { value: query, onChange: (e) => setQuery(e.target.value), placeholder: "Describe what you want" }), _jsx(Input, { type: "number", min: 1, max: 50, value: topK, onChange: (e) => setTopK(Number(e.target.value)), className: "max-w-24" }), _jsx(Button, { onClick: () => void runSearch(), children: "Search" })] }), _jsx("div", { className: "mt-4 max-h-64 overflow-auto rounded border border-border", children: _jsxs("table", { className: "w-full text-sm", children: [_jsx("thead", { className: "sticky top-0 bg-muted", children: _jsxs("tr", { children: [_jsx("th", { className: "p-2 text-left", children: "Item" }), _jsx("th", { className: "p-2 text-left", children: "Category" }), _jsx("th", { className: "p-2 text-left", children: "Score" })] }) }), _jsx("tbody", { children: semanticRows.map((row) => (_jsxs("tr", { className: "border-t border-border", children: [_jsx("td", { className: "p-2", children: row.name }), _jsx("td", { className: "p-2", children: row.category }), _jsx("td", { className: "p-2", children: row.score.toFixed(4) })] }, row.itemId))) })] }) })] }), _jsxs(Card, { children: [_jsx(CardTitle, { children: "Top Sellers" }), _jsx("div", { className: "mt-4 max-h-64 overflow-auto rounded border border-border", children: _jsxs("table", { className: "w-full text-sm", children: [_jsx("thead", { className: "sticky top-0 bg-muted", children: _jsxs("tr", { children: [_jsx("th", { className: "p-2 text-left", children: "Name" }), _jsx("th", { className: "p-2 text-left", children: "Units" }), _jsx("th", { className: "p-2 text-left", children: "Revenue" })] }) }), _jsx("tbody", { children: topSellers.map((row) => (_jsxs("tr", { className: "border-t border-border", children: [_jsx("td", { className: "p-2", children: row.name }), _jsx("td", { className: "p-2", children: row.unitsSold }), _jsxs("td", { className: "p-2", children: ["$", row.revenue.toFixed(2)] })] }, row.itemId))) })] }) })] }), _jsxs(Card, { children: [_jsx(CardTitle, { children: "Place Order" }), _jsxs("div", { className: "mt-4 grid gap-2 sm:grid-cols-3", children: [_jsx(Input, { type: "number", min: 1, value: orderItemId, onChange: (e) => setOrderItemId(Number(e.target.value)) }), _jsx(Input, { type: "number", min: 1, value: orderQty, onChange: (e) => setOrderQty(Number(e.target.value)) }), _jsx(Button, { onClick: () => void placeOrder(), children: "Submit" })] }), orderMessage && _jsx("p", { className: "mt-3 text-sm text-neutral-700", children: orderMessage })] })] })] }));
}
