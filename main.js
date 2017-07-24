

let ogr2ogr = require('ogr2ogr')
let fs = require('fs')

var shapefile = ogr2ogr('./output/sewer_drains_311calls.geojson')
                    .format('ESRI Shapefile')
                    .skipfailures()
                    .stream()
shapefile.pipe(fs.createWriteStream('./output/drains_sewers_311_shapefile.zip'))
