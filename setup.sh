#!/bin/bash
# ═══════════════════════════════════════════
#  setup.sh — Run ONCE after docker compose up
#  Creates the Redpanda topic for gold prices
# ═══════════════════════════════════════════

echo "⏳ Waiting for Redpanda to be ready..."
sleep 5

echo "📌 Creating topic: gold-price-ticks"
docker exec redpanda rpk topic create gold-price-ticks \
  --partitions 1 \
  --replicas 1

echo "📌 Creating topic: gold-indicators"
docker exec redpanda rpk topic create gold-indicators \
  --partitions 1 \
  --replicas 1

echo ""
echo "✅ Topics created! Verify with:"
echo "   docker exec redpanda rpk topic list"
echo ""
echo "🌐 Open these in your browser:"
echo "   QuestDB Console   → http://localhost:9000"
echo "   Redpanda Console  → http://localhost:8080"
echo "   Grafana           → http://localhost:3000  (admin/admin)"