import sqlite3
c=sqlite3.connect('data/entsoe_data.db')
print("Monthly avg ROR MW by year")
print("mon   2023  2024  2025  2026")
for m in range(1,13):
    row=f"{m:02d} "
    for y in (2023,2024,2025,2026):
        v=c.execute("select avg(value_mw) from generation where psr_type='Hydro Run-of-river and poundage' and substr(timestamp,1,7)=?",(f'{y}-{m:02d}',)).fetchone()[0]
        row+= f"{v:6.0f}" if v else "     -"
    print(row)
print("\nMonthly total gen mix 2026-07 vs 2025-07 (avg MW)")
for t in ['Hydro Run-of-river and poundage','Hydro Water Reservoir','Hydro Pumped Storage','Solar','Wind Onshore','Fossil Gas','Biomass','Waste','Other']:
    a=c.execute("select avg(value_mw) from generation where psr_type=? and substr(timestamp,1,7)='2026-07'",(t,)).fetchone()[0]
    b=c.execute("select avg(value_mw) from generation where psr_type=? and substr(timestamp,1,7)='2025-07'",(t,)).fetchone()[0]
    print(f"{t:34s} {a:7.0f} {b:7.0f}  {a-b:+7.0f}")
print("\nnet imports as share of load, July 2026")
for d,imp,ld in c.execute("""select substr(f.timestamp,1,7), avg(f.net), avg(l.load_mw) from
 (select timestamp, sum(import_mw)-sum(export_mw) net from cross_border_flows group by timestamp) f
 join load l on l.timestamp=f.timestamp group by 1 order by 1"""):
    print(f"  {d}: net imp {imp:6.0f} MW = {100*imp/ld:4.1f}% of load {ld:.0f}")
