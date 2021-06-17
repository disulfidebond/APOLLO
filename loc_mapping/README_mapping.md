# Location Mapping Strategy README

The goal in the mapping strategy is to search a large area of Wisconsin accurately, with the least amount of redundancy, using search zones created by the Google Maps API to find locations that match to a given name. The Google Places API attempts to find a match to a name provided by contact tracing, which matches to a database of business names. This task is somewhat complicated by Google Maps API only allowing circular search radii, and that the Earth is not flat. The former is the focus of this writeup, while the latter can be solved by using the haversine formula. 
The Google Places API requires searches to be within a circular zone with a maximum radius of 30 km. Multiple successive searches are permitted, although this will increase API costs. In the simplified example below, the boxed-in area needs to be completely searched using circles. The shaded areas are covered by at least one search, while the blue areas are missed.
![](https://github.com/disulfidebond/APOLLO/blob/main/media/mapping_img1.png)
In theory, this problem could be resolved by simply saturating the search box with search circles, however, this would not be a strategy with the least amount of redundancy, and would drive up the cost from excessive API calls.

A more beneficial approach would be to create a grid of overlapping search circles. This would have the added benefit of allowing the search zone to be expanded in an organized manner if no results are found in the initial search zone. A simplified example is shown below utilizes overlapping circles to scan the blue rectangle area, and employs a hexagon grid pattern as the framework for the circular gray search regions:

![](https://github.com/disulfidebond/APOLLO/blob/main/media/mapping_img2.png)

This pattern can be scaled to cover a wide area where the exact location is unknown. Consider the following (fictitious) example, where a cluster of cases in Dane County identifies a black dot centroid of these cases near Sun Prairie.
![](https://github.com/disulfidebond/APOLLO/blob/main/media/mapping_img3.png)

The initial search would be limited by the Google Places API to a 30 km circular zone:
![](https://github.com/disulfidebond/APOLLO/blob/main/media/mapping_img4.png)

Using the grid strategy described above, expanding search zones can be employed to a circular ring outside of the initial search zone, while ensuring there are no gaps in the search grid. 
![](https://github.com/disulfidebond/APOLLO/blob/main/media/mapping_img5.png)


The hexagon framework for the searches serves to create an organized grid where each Google Places API will center and originate the search for a potential matching business name. 
![](https://github.com/disulfidebond/APOLLO/blob/main/media/mapping_img6.png)


As long as the central search radius (the circle that is inscribed within each hexagon grid unit) is greater than or equal to the search radii on the hexagon vertices, the overlaps will ensure that all areas under consideration will be searched at least once for a matching business name.

As a final note, using the haversine formula to generate the hexagonal grid on the Earth’s surface results in a linear decrease in accuracy due to rounding areas when calculations are made. In practice, this is not a concern, since the overlapping search circles will compensate for this error as long as the distance from the centroid does not exceed 500 km.
