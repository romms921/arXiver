"""
MapManager: A utility class for creating consistent, reusable Folium maps.
Centralizes map styling, GeoJSON handling, and legend generation.
"""

import folium
from folium import plugins
import matplotlib.pyplot as plt
from matplotlib import colors as mpl_colors
import numpy as np
from collections import Counter
from typing import Dict, List, Optional, Tuple, Any, Callable
from IPython.display import HTML, IFrame
import base64

# GeoJSON URL for country boundaries
GEOJSON_URL = "http://geojson.xyz/naturalearth-3.3.0/ne_50m_admin_0_countries.geojson"

# Standard country name mapping for GeoJSON compatibility
COUNTRY_NAME_MAPPING = {
    'USA': 'United States',
    'United States of America': 'United States',
    'UK': 'United Kingdom',
    'Czech Republic': 'Czech Rep.',
    'South Korea': 'Korea',
    'Vatican City': 'Vatican',
    'The Netherlands': 'Netherlands',
}

# Reverse mapping for GeoJSON -> DataFrame
GEOJSON_TO_DATA_MAPPING = {
    'United States of America': 'United States',
    'Czech Rep.': 'Czech Republic',
    'Dem. Rep. Korea': 'South Korea',
    'Korea': 'South Korea',
}


def display_html(location: str):
    """
    Display a saved HTML map file within a Jupyter notebook.
    
    Args:
        location: Path to the HTML file.
    """
    with open(location, "r", encoding='utf-8') as f:
        html_content = f.read()
    
    html_str = "data:text/html;base64," + base64.b64encode(html_content.encode('utf-8')).decode('utf-8')
    return IFrame(src=html_str, width='100%', height='500px')


def get_colormap(name: str, n_colors: Optional[int] = None):
    """
    Get a colormap using the modern matplotlib API (avoids deprecation warning).
    
    Args:
        name: Name of the colormap (e.g., 'jet', 'viridis').
        n_colors: Optional number of colors to resample to.
    
    Returns:
        A matplotlib colormap.
    """
    cmap = plt.colormaps.get_cmap(name)
    if n_colors is not None:
        cmap = cmap.resampled(n_colors)
    return cmap


def get_hex_colors(cmap_name: str, n_colors: int) -> List[str]:
    """
    Generate a list of hex colors from a colormap.
    
    Args:
        cmap_name: Name of the colormap.
        n_colors: Number of colors to generate.
    
    Returns:
        List of hex color strings.
    """
    cmap = get_colormap(cmap_name, n_colors)
    return [mpl_colors.to_hex(cmap(i)) for i in range(n_colors)]


class MapManager:
    """
    A utility class for creating Folium maps with consistent styling.
    
    Supports:
    - Choropleth maps (numerical data)
    - Categorical maps (color by category)
    - Pie chart marker maps (subject distributions)
    - Collaboration line maps
    - Custom legends
    """
    
    def __init__(
        self,
        location: Tuple[float, float] = (20, 0),
        zoom_start: float = 2.3,
        tiles: str = 'cartodb positron'
    ):
        """Initialize a new Folium map with default styling."""
        self.map = folium.Map(
            location=location,
            zoom_start=zoom_start,
            tiles=tiles
        )
        self.geojson_url = GEOJSON_URL
    
    @staticmethod
    def normalize_country_name(name: str, for_geojson: bool = True) -> str:
        """
        Normalize country names for consistency.
        
        Args:
            name: The country name to normalize.
            for_geojson: If True, map to GeoJSON names. If False, map from GeoJSON.
        
        Returns:
            The normalized country name.
        """
        if for_geojson:
            return COUNTRY_NAME_MAPPING.get(name, name)
        else:
            return GEOJSON_TO_DATA_MAPPING.get(name, name)
    
    @staticmethod
    def create_svg_pie(dist: Dict[str, int], color_map: Dict[str, str], size: int = 30) -> str:
        """
        Create an SVG pie chart for marker icons.
        
        Args:
            dist: Dictionary of {category: count}.
            color_map: Dictionary of {category: hex_color}.
            size: Size of the pie chart in pixels.
        
        Returns:
            SVG string for the pie chart.
        """
        total = sum(dist.values())
        if total == 0:
            return ''
        
        svg = f'<svg viewBox="-1 -1 2 2" style="width:{size}px; height:{size}px;">'
        last_angle = 0
        
        for cat, count in dist.items():
            percentage = count / total
            angle = percentage * 2 * np.pi
            x1, y1 = np.cos(last_angle), np.sin(last_angle)
            x2, y2 = np.cos(last_angle + angle), np.sin(last_angle + angle)
            large_arc = 1 if percentage > 0.5 else 0
            color = color_map.get(cat, '#000000')
            svg += f'<path d="M 0 0 L {x1} {y1} A 1 1 0 {large_arc} 1 {x2} {y2} Z" fill="{color}" stroke="white" stroke-width="0.02" />'
            last_angle += angle
        
        svg += '</svg>'
        return svg
    
    def add_categorical_geojson(
        self,
        data_dict: Dict[str, Any],
        color_map: Dict[str, str],
        get_category_fn: Callable,
        default_color: str = '#ffffff',
        default_opacity: float = 0.1
    ):
        """
        Add a GeoJSON layer with colors based on categorical data.
        
        Args:
            data_dict: Dictionary mapping country names to data.
            color_map: Dictionary mapping categories to hex colors.
            get_category_fn: Function that takes data and returns the category.
            default_color: Color for countries without data.
            default_opacity: Opacity for countries without data.
        """
        def style_function(feature):
            country_name = feature['properties']['name']
            std_name = self.normalize_country_name(country_name, for_geojson=False)
            
            data = data_dict.get(std_name)
            if data:
                category = get_category_fn(data)
                return {
                    'fillColor': color_map.get(category, default_color),
                    'color': 'black',
                    'weight': 1,
                    'fillOpacity': 0.6
                }
            return {
                'fillColor': default_color,
                'color': 'black',
                'weight': 1,
                'fillOpacity': default_opacity
            }
        
        folium.GeoJson(
            self.geojson_url,
            style_function=style_function,
            tooltip=folium.GeoJsonTooltip(fields=['name'], aliases=['Country:'])
        ).add_to(self.map)
    
    def add_choropleth(
        self,
        data_df,
        country_col: str,
        value_col: str,
        color_scale: str = 'YlOrRd',
        legend_name: str = 'Value',
        nan_fill_color: str = 'white'
    ):
        """
        Add a choropleth layer to the map.
        
        Args:
            data_df: DataFrame with country and value columns.
            country_col: Name of the column containing country names.
            value_col: Name of the column containing values.
            color_scale: Color scale name (e.g., 'YlOrRd', 'YlGnBu').
            legend_name: Legend title.
            nan_fill_color: Color for countries without data.
        """
        # Map country names for GeoJSON compatibility
        data_df = data_df.copy()
        data_df['country_mapped'] = data_df[country_col].apply(
            lambda x: self.normalize_country_name(x, for_geojson=True)
        )
        
        folium.Choropleth(
            geo_data=self.geojson_url,
            data=data_df,
            columns=['country_mapped', value_col],
            key_on='feature.properties.name',
            fill_color=color_scale,
            fill_opacity=0.7,
            line_opacity=0.2,
            nan_fill_color=nan_fill_color,
            legend_name=legend_name,
            color='black'
        ).add_to(self.map)
    
    def add_collaboration_lines(
        self,
        pair_counts: Dict[Tuple[str, str], int],
        coords_df,
        color_map: Optional[Dict[str, str]] = None,
        threshold: int = 0,
        opacity: float = 0.3,
        max_weight: float = 10
    ):
        """
        Add collaboration lines between countries.
        
        Args:
            pair_counts: Dictionary {(country1, country2): count}.
            coords_df: DataFrame with 'country', 'latitude', 'longitude' columns.
            color_map: Optional {country: color} map. Uses first country's color.
            threshold: Minimum count to draw a line.
            opacity: Line opacity.
            max_weight: Maximum line weight.
        """
        for (c1, c2), count in pair_counts.items():
            if count < threshold:
                continue
            
            coords1 = coords_df[coords_df['country'] == c1]
            coords2 = coords_df[coords_df['country'] == c2]
            
            if coords1.empty or coords2.empty:
                continue
            
            lat1 = coords1['latitude'].values[0]
            lon1 = coords1['longitude'].values[0]
            lat2 = coords2['latitude'].values[0]
            lon2 = coords2['longitude'].values[0]
            
            # Determine color
            color = 'blue'
            if color_map:
                color = color_map.get(c1, color_map.get(c2, 'blue'))
            
            # Logarithmic weight scaling
            weight = min(1 + np.log1p(count), max_weight)
            
            folium.PolyLine(
                locations=[(lat1, lon1), (lat2, lon2)],
                color=color,
                weight=weight,
                opacity=opacity,
                popup=f"<b>{c1} ↔ {c2}</b><br>Collaborations: {count}"
            ).add_to(self.map)
    
    def add_pie_markers(
        self,
        data: List[Tuple[float, float, Dict[str, int]]],
        color_map: Dict[str, str],
        size: int = 30
    ):
        """
        Add pie chart markers to the map.
        
        Args:
            data: List of (latitude, longitude, distribution_dict) tuples.
            color_map: Dictionary mapping categories to hex colors.
            size: Size of pie charts in pixels.
        """
        for lat, lon, dist in data:
            svg = self.create_svg_pie(dist, color_map, size)
            if svg:
                icon = folium.DivIcon(html=svg)
                folium.Marker(
                    location=[lat, lon],
                    icon=icon
                ).add_to(self.map)
    
    def add_legend(
        self,
        title: str,
        items: Dict[str, str],
        position: str = 'bottomleft'
    ):
        """
        Add a custom HTML legend to the map.
        
        Args:
            title: Legend title.
            items: Dictionary of {label: hex_color}.
            position: Position ('bottomleft', 'bottomright', etc.).
        """
        pos_styles = {
            'bottomleft': 'bottom: 50px; left: 50px;',
            'bottomright': 'bottom: 50px; right: 50px;',
            'topleft': 'top: 50px; left: 50px;',
            'topright': 'top: 50px; right: 50px;'
        }
        
        pos_style = pos_styles.get(position, pos_styles['bottomleft'])
        
        legend_html = f'''
        <div style="position: fixed; {pos_style} width: 250px; height: auto; 
                    border:2px solid grey; z-index:9999; font-size:12px;
                    background-color:white; opacity: 0.9; padding: 10px; 
                    max-height: 400px; overflow-y: auto;">
        <b>{title}</b><br>
        '''
        
        for label, color in items.items():
            legend_html += f'<i style="background:{color}; width:12px; height:12px; float:left; margin-right:5px; border: 1px solid black;"></i>{label}<br>'
        
        legend_html += '</div>'
        
        self.map.get_root().html.add_child(folium.Element(legend_html))
    
    def add_gradient_legend(
        self,
        title: str,
        low_label: str = 'Low',
        high_label: str = 'High',
        position: str = 'bottomright'
    ):
        """
        Add a gradient legend (for jet colormap) to the map.
        
        Args:
            title: Legend title.
            low_label: Label for low end.
            high_label: Label for high end.
            position: Position on the map.
        """
        pos_styles = {
            'bottomleft': 'bottom: 50px; left: 50px;',
            'bottomright': 'bottom: 20px; right: 20px;',
        }
        pos_style = pos_styles.get(position, pos_styles['bottomright'])
        
        legend_html = f'''
        <div id='maplegend' class='maplegend' 
            style='position: absolute; z-index:9999; border:2px solid grey; background-color:rgba(255, 255, 255, 0.8);
                   border-radius:6px; padding: 10px; font-size:12px; {pos_style}'>
        <div class='legend-title'><b>{title}</b></div>
        <div class='legend-scale' style='margin-bottom: 5px;'>
          <div style='background: linear-gradient(to right, blue, cyan, green, yellow, red); height: 10px; width: 100%;'></div>
          <div style='display: flex; justify-content: space-between; font-size: 10px;'>
            <span>{low_label}</span>
            <span>{high_label}</span>
          </div>
        </div>
        </div>
        '''
        self.map.get_root().html.add_child(folium.Element(legend_html))
    
    def save(self, filename: str) -> str:
        """Save the map to an HTML file and return the filename."""
        self.map.save(filename)
        return filename
    
    def display(self, filename: str, height: str = '600px'):
        """Save and display the map in a Jupyter notebook."""
        self.save(filename)
        return IFrame(src=filename, width='100%', height=height)
    
    def get_map(self) -> folium.Map:
        """Return the underlying Folium map object."""
        return self.map
