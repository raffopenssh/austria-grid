#!/usr/bin/env python3
"""Analyze cadastre enrichment results."""

import json
from collections import defaultdict, Counter

INPUT_FILE = "data/all_power_plants_with_cadastre.json"

def main():
    print("\n" + "="*60)
    print("CADASTRE ENRICHMENT ANALYSIS")
    print("="*60 + "\n")
    
    # Load data
    with open(INPUT_FILE, "r") as f:
        data = json.load(f)
    
    features = data["features"]
    total = len(features)
    
    # Count enrichment success
    with_cadastre = 0
    without_cadastre = 0
    by_source = defaultdict(lambda: {"total": 0, "with_cadastre": 0})
    cadastral_communities = Counter()
    parcel_statuses = Counter()
    
    for feature in features:
        props = feature["properties"]
        source = props.get("source", "unknown")
        cadastre = props.get("cadastre")
        
        by_source[source]["total"] += 1
        
        if cadastre:
            with_cadastre += 1
            by_source[source]["with_cadastre"] += 1
            
            if cadastre.get("cadastral_community"):
                cadastral_communities[cadastre["cadastral_community"]] += 1
            if cadastre.get("status"):
                parcel_statuses[cadastre["status"]] += 1
        else:
            without_cadastre += 1
    
    # Print summary
    print(f"Total power plants: {total}")
    print(f"With cadastre data: {with_cadastre} ({with_cadastre/total*100:.1f}%)")
    print(f"Without cadastre data: {without_cadastre} ({without_cadastre/total*100:.1f}%)")
    print("\n" + "-"*60 + "\n")
    
    print("By Power Source:")
    for source in sorted(by_source.keys()):
        stats = by_source[source]
        pct = stats["with_cadastre"] / stats["total"] * 100 if stats["total"] > 0 else 0
        print(f"  {source:20s}: {stats['with_cadastre']:4d}/{stats['total']:4d} ({pct:5.1f}%)")
    
    print("\n" + "-"*60 + "\n")
    
    print("Top 10 Cadastral Communities (by power plant count):")
    for kg, count in cadastral_communities.most_common(10):
        print(f"  KG {kg}: {count} power plants")
    
    print("\n" + "-"*60 + "\n")
    
    print("Parcel Status Distribution:")
    for status, count in parcel_statuses.most_common():
        pct = count / with_cadastre * 100 if with_cadastre > 0 else 0
        print(f"  {status}: {count} ({pct:.1f}%)")
    
    print("\n" + "="*60 + "\n")

if __name__ == "__main__":
    main()
