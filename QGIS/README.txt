How the ROW was determined using QGIS and ArcGIS- Step by Step


Percentage ROW whole line:


QGIS:
1. change coordinate system from bus line to EPSG:3011
2. use "split line by maximum length" to segmentise bus line in QGIS, used length: 1m
3. export 1m -linesegments  as gpkg file 
ARCGIS:
4. open 1m-segments as gpkg file in ARCGIS
5. convert gpkg with "Copy Features" tool (output name: ex. segments_arcgis_line61_Hornsberg_1m)
6 use near function with input feature 1m segmentlayer, near feature kollektivbuslanes (renamed: street with right of way for buses),use 10m search radius
7. open attribute table of 1m segment layer(ex segments_arcgis_line61_Hornsberg_1m), select by attributes NEARFID!=-1 -->divide NEARFID !=-1 /total rows -->percentage


Result:
Bus line 61 (Hornsberg-->Ruddammen): 26%
Bus line 53 (Karolinska Institutet >Henriksdalsberget): 28%
Bus line 74: (Sickla ->Hornsberg)41%
Bus line 61: ->Hornsberg: 24%
Bus line 53: (->Karolinska): 30%
Bus line 74:->Sickla: 42%



Percentage ROW per segment:

ARCGIS:
1. get csv table with coordinates from Python (ex coordinates_61_Hornsberg)
2. Put csv table in arcgis- create pointlayer (ex Coordinates_61_Hornsberg_XYTabletoPoint, renamed: bus stops line 61 to Hornsberg ) from it, WGS 1984 as reference system
3. split busline into segments with "split line at points" (ex: input line61_Hornsberg_samecoordinatesystem and pointlayer Coordinates_61_Hornsberg_XYTabletoPoint), name result line61_Hornsberg_stopsegments
4. use "Feature vertices to points" (input : line61_Ruddammen_stopsegments), select only end vertices!, name result: line61_segmentende
5. spatial join (right click on Target feature ) with line61_segmentende as Target feature and pointlayer (Coordinates_61_Hornsberg_XYTabletoPoint) as join feature, distance 50m  
6. normal join (right click) with line61_Hornsberg_stopsegments as Inputtable and line61_Hornsberg_segmentende as join table,  objectid as attribute for join for both layers
7 add is-ROW field in 1m segments layer(if not already done earlier) (ex segments_arcgis_line61_Hornsberg_1m)
def set_flag(near_fid):
    if near_fid == -1:
        return -1        -->no ROW
    else:
        return 0         --> ROW
8. near function for 1m segments (ex segments_arcgis_line61_Hornsberg_1m), near feature is stopsegments (ex:line61_Hornsberg_stopsegments), Distanz 10 m? ,change name to NEAR_FID_segments (to not confuse with already existining NEAR_FID field)!! 
9. Summary Statistic tool- Input segments 1m (ex segments_arcgis_line61_Hornsberg_1m), Output name segments_line61_Hornsberg_rowpersegment statistical field is ROW , case field NEAR_FID_segments
10. create new column called percentage in same table (ex: segments_line61_Hornsberg_rowpersegment)
calculate percentage in same table:	
	1-((-!SUM_ROW!)/!FREQUENCY!)

11. join back to stopsegments, Input (right click) stopsegments-layer(ex:line61_Hornsberg_stopsegments), field: Object_id, join table: rowpersegmenttabelle(ex:segments_line61_Hornsberg_rowpersegment), field: NEAR_FID_segments
12. join back to XY table (ex:Coordinates_61_Hornsberg_XYTabletoPoint, renamed bus stops line 61 to Hornsberg)
-rightclick on XY table, join feature is stopsegments (ex:line61_Hornsberg_stopsegments), field:stopsequence for both
13. export (remove all unnecessary columns) as row_percentage_segment_line61_Hornsberg


-->every additional information provided in a table (ex: average speed in peak hours) can be joined by using the stopsequence as Join attribute on the stop-segments layer



