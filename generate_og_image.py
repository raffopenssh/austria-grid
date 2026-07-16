#!/usr/bin/env python3
"""Generate OpenGraph images for Austria Wind Grid"""

from PIL import Image, ImageDraw, ImageFont
import json
import os

# Image dimensions for OG (1200x630 is optimal)
WIDTH = 1200
HEIGHT = 630

# Colors
BG_COLOR = (26, 26, 46)  # #1a1a2e
ACCENT_COLOR = (0, 212, 170)  # #00d4aa
TEXT_COLOR = (234, 234, 234)  # #eaeaea
SECONDARY_TEXT = (136, 136, 136)  # #888

def create_main_og_image():
    """Create the main OG image for the homepage"""
    img = Image.new('RGB', (WIDTH, HEIGHT), BG_COLOR)
    draw = ImageDraw.Draw(img)
    
    # Try to use a system font, fallback to default
    try:
        title_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 60)
        subtitle_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 32)
        stats_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 48)
        label_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 24)
    except:
        title_font = ImageFont.load_default()
        subtitle_font = title_font
        stats_font = title_font
        label_font = title_font
    
    # Draw gradient-like background with shapes
    for i in range(0, WIDTH, 50):
        opacity = int(20 + (i / WIDTH) * 30)
        draw.line([(i, 0), (i + 200, HEIGHT)], fill=(22, 33, 62, opacity), width=2)
    
    # Draw wind turbine icons (simplified)
    turbine_positions = [(100, 450), (250, 420), (1050, 480), (1100, 400)]
    for x, y in turbine_positions:
        # Tower
        draw.rectangle([x-3, y, x+3, y+100], fill=(60, 60, 80))
        # Blades (triangle approximation)
        draw.polygon([(x, y-5), (x-40, y+20), (x, y+10)], fill=ACCENT_COLOR)
        draw.polygon([(x, y-5), (x+35, y+25), (x, y+10)], fill=ACCENT_COLOR)
        draw.polygon([(x, y-5), (x+5, y-50), (x, y+10)], fill=ACCENT_COLOR)
    
    # Title
    title = "Windkraft Österreich"
    draw.text((WIDTH//2, 100), title, font=title_font, fill=ACCENT_COLOR, anchor="mm")
    
    # Subtitle
    subtitle = "Netzkapazität für neue Windenergieanlagen"
    draw.text((WIDTH//2, 170), subtitle, font=subtitle_font, fill=TEXT_COLOR, anchor="mm")
    
    # Load stats from data
    try:
        with open('data/wind_turbines_enhanced.json') as f:
            turbines = json.load(f)
        with open('data/transformer_stations.json') as f:
            transformers = json.load(f)
        total_turbines = len(turbines)
        total_mw = sum(t.get('estimated_mw', 0) for t in turbines)
        total_transformers = len(transformers)
    except:
        total_turbines = 1400
        total_mw = 4200
        total_transformers = 150
    
    # Stats boxes
    stats = [
        (f"{total_turbines:,}".replace(",", "."), "Windkraftanlagen"),
        (f"{int(total_mw):,} MW".replace(",", "."), "Installierte Leistung"),
        (f"{total_transformers}", "Umspannwerke"),
    ]
    
    box_width = 280
    total_width = len(stats) * box_width + (len(stats)-1) * 40
    start_x = (WIDTH - total_width) // 2
    
    for i, (value, label) in enumerate(stats):
        x = start_x + i * (box_width + 40) + box_width // 2
        y = 320
        
        # Box background
        draw.rounded_rectangle(
            [x - box_width//2, y - 50, x + box_width//2, y + 80],
            radius=15,
            fill=(22, 33, 62)
        )
        
        # Value
        draw.text((x, y), value, font=stats_font, fill=ACCENT_COLOR, anchor="mm")
        
        # Label
        draw.text((x, y + 50), label, font=label_font, fill=SECONDARY_TEXT, anchor="mm")
    
    # Footer
    footer = "austria-power.exe.xyz"
    draw.text((WIDTH//2, HEIGHT - 40), footer, font=label_font, fill=SECONDARY_TEXT, anchor="mm")
    
    # Border accent
    draw.rectangle([0, 0, WIDTH, 6], fill=ACCENT_COLOR)
    
    img.save('static/og-image.png', 'PNG', optimize=True)
    print("Created static/og-image.png")

def create_district_og_image(district_name, iso, stats):
    """Create OG image for a specific district"""
    img = Image.new('RGB', (WIDTH, HEIGHT), BG_COLOR)
    draw = ImageDraw.Draw(img)
    
    try:
        title_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 52)
        subtitle_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 28)
        stats_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 42)
        label_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 22)
    except:
        title_font = ImageFont.load_default()
        subtitle_font = title_font
        stats_font = title_font
        label_font = title_font
    
    # Title
    draw.text((WIDTH//2, 80), f"Windkraft in {district_name}", font=title_font, fill=ACCENT_COLOR, anchor="mm")
    draw.text((WIDTH//2, 140), "Netzkapazität für neue Anlagen", font=subtitle_font, fill=TEXT_COLOR, anchor="mm")
    
    # Capacity score indicator
    score = stats.get('capacity_score', 50)
    if score >= 70:
        score_color = ACCENT_COLOR
        score_text = "Hohe Kapazität"
    elif score >= 30:
        score_color = (255, 217, 61)  # yellow
        score_text = "Mittlere Kapazität"
    else:
        score_color = (255, 107, 107)  # red
        score_text = "Geringe Kapazität"
    
    # Capacity bar
    bar_y = 200
    bar_width = 600
    bar_x = (WIDTH - bar_width) // 2
    draw.rounded_rectangle([bar_x, bar_y, bar_x + bar_width, bar_y + 30], radius=15, fill=(60, 60, 80))
    draw.rounded_rectangle([bar_x, bar_y, bar_x + int(bar_width * score / 100), bar_y + 30], radius=15, fill=score_color)
    draw.text((WIDTH//2, bar_y + 60), f"{score_text} ({score}%)", font=subtitle_font, fill=score_color, anchor="mm")
    
    # Stats
    stat_items = [
        (f"{stats.get('installed_mw', 0):.1f} MW", "Installiert"),
        (str(stats.get('turbines', 0)), "Anlagen"),
        (f"{stats.get('estimated_available_mw', 0):.1f} MW", "Verfügbar"),
    ]
    
    box_width = 250
    total_w = len(stat_items) * box_width + (len(stat_items)-1) * 30
    start_x = (WIDTH - total_w) // 2
    
    for i, (value, label) in enumerate(stat_items):
        x = start_x + i * (box_width + 30) + box_width // 2
        y = 400
        draw.rounded_rectangle([x - box_width//2, y - 40, x + box_width//2, y + 60], radius=12, fill=(22, 33, 62))
        draw.text((x, y), value, font=stats_font, fill=TEXT_COLOR, anchor="mm")
        draw.text((x, y + 40), label, font=label_font, fill=SECONDARY_TEXT, anchor="mm")
    
    # Footer
    draw.text((WIDTH//2, HEIGHT - 40), "austria-power.exe.xyz", font=label_font, fill=SECONDARY_TEXT, anchor="mm")
    draw.rectangle([0, 0, WIDTH, 6], fill=score_color)
    
    # Ensure directory exists
    os.makedirs('static/og', exist_ok=True)
    filename = f'static/og/bezirk-{iso.lower().replace(" ", "-")}.png'
    img.save(filename, 'PNG', optimize=True)
    return filename

def create_analytics_og_image():
    """OG image for the /analytics market dashboard"""
    img = Image.new('RGB', (WIDTH, HEIGHT), BG_COLOR)
    draw = ImageDraw.Draw(img)
    try:
        title_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 56)
        subtitle_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 30)
        label_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 24)
    except Exception:
        title_font = ImageFont.load_default()
        subtitle_font = title_font
        label_font = title_font

    # Stylized duck-curve style line chart
    import math
    points = []
    for i in range(25):
        x = 100 + i * (WIDTH - 200) / 24
        # duck curve-ish shape
        v = 60 + 30 * math.sin(i / 24 * 2 * math.pi - 1.5) - 45 * math.exp(-((i - 13) ** 2) / 8)
        y = 520 - v * 2.2
        points.append((x, y))
    # area fill
    draw.polygon(points + [(points[-1][0], 560), (points[0][0], 560)], fill=(0, 80, 66))
    draw.line(points, fill=ACCENT_COLOR, width=5)
    # zero line (negative prices)
    draw.line([(100, 460), (WIDTH - 100, 460)], fill=(255, 107, 107), width=2)
    draw.text((WIDTH - 105, 445), "0 €/MWh", font=label_font, fill=(255, 107, 107), anchor="rm")

    draw.text((WIDTH // 2, 110), "Strommarkt-Analytics Österreich", font=title_font, fill=ACCENT_COLOR, anchor="mm")
    draw.text((WIDTH // 2, 180), "Negative Preise · Capture Rates · Speicher-Arbitrage · Duck Curve",
              font=subtitle_font, fill=TEXT_COLOR, anchor="mm")
    draw.text((WIDTH // 2, 230), "3,5 Jahre ENTSO-E Daten seit 2023", font=label_font, fill=SECONDARY_TEXT, anchor="mm")
    draw.text((WIDTH // 2, HEIGHT - 35), "austria-power.exe.xyz/analytics", font=label_font, fill=SECONDARY_TEXT, anchor="mm")
    draw.rectangle([0, 0, WIDTH, 6], fill=ACCENT_COLOR)
    img.save('static/og-analytics.png', 'PNG', optimize=True)
    print("Created static/og-analytics.png")


def create_all_district_images():
    import capacity_stats
    stats = capacity_stats.compute_district_stats()
    for iso, s in stats.items():
        create_district_og_image(s['name'], iso, s)
    print(f"Created {len(stats)} district OG images in static/og/")


if __name__ == '__main__':
    import sys
    os.chdir('/home/exedev/austria-grid')
    create_main_og_image()
    create_analytics_og_image()
    if '--districts' in sys.argv:
        create_all_district_images()
