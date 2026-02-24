#!/bin/bash

echo "=== Cadastre Enrichment Progress ==="
echo ""

if [ -f cadastre_enrichment.log ]; then
    echo "Last 10 log entries:"
    tail -10 cadastre_enrichment.log
    echo ""
    
    # Count progress
    PROCESSED=$(grep -c "Processing" cadastre_enrichment.log)
    SUCCESS=$(grep -c "✓ Found parcel" cadastre_enrichment.log)
    
    echo "Stats:"
    echo "  Processed: $PROCESSED / 4727"
    echo "  Found parcels: $SUCCESS"
    
    if [ $PROCESSED -gt 0 ]; then
        PERCENT=$(echo "scale=2; $PROCESSED * 100 / 4727" | bc)
        echo "  Progress: ${PERCENT}%"
    fi
    
    echo ""
    echo "Output file size:"
    ls -lh data/all_power_plants_with_cadastre.json 2>/dev/null || echo "  Not created yet"
else
    echo "Log file not found"
fi

echo ""
echo "Tmux session status:"
tmux ls 2>/dev/null | grep cadastre || echo "  No cadastre session running"
