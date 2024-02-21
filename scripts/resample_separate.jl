# This script is a post-processing script to resample the european level forest change values to the webmercator projection
using Glob
using Rasters
using NCDatasets
using Extents
using YAXArrays
using ArchGDAL

#forestpaths = glob("*.nc", "/eodc/private/pangeojulia/forestaggregated/")

#foresttiles = [match(r"E\d\d\dN\d\d\dT3", c).match for c in forestpaths]

#foresttuples = [(east=parse(Int, t[2:4]), north=parse(Int, t[6:8])) for t in foresttiles]

clusterpaths = glob("*.nc", "/eodc/private/pangeojulia/rqatrend_EU/forestmasked_thresh_cluster")

resampledir = "/eodc/private/pangeojulia/rqatrend_EU/webmercator"
mkpath(resampledir)
equi7_eu_projstring = "+proj=aeqd +lat_0=53 +lon_0=24 +x_0=5837287.81977 +y_0=2121415.69617 +datum=WGS84 +units=m +no_defs"
equi7_eu = ProjString(equi7_eu_projstring)

for c in clusterpaths
    outpath = joinpath(resampledir, "webmercator_" * basename(c))
    if isfile(outpath)
        continue
    end
    @time "Loading" ras = Raster(c, lazy=true;crs=equi7_eu, mappedcrs=equi7_eu, missingval=0)
    @time "Replace" rasmiss = replace(ras , missing=>0)
    @time "Resampling" ras_webmercator = Rasters.resample(rasmiss,crs=EPSG(3857))
    @time "Writing" write(outpath, ras_webmercator, deflatelevel=5, missingval=0, force=true)
    @time "GC" GC.gc()
end

