using Glob
using Rasters
using NCDatasets
foresttiles = glob("*.nc", "/eodc/private/pangeojulia/forestaggregated/")
threshdir = "/eodc/private/pangeojulia/rqatrend_EU/forestmasked_thresh_cluster/"
rqadir = "/eodc/private/pangeojulia/rqatrend_EU/"
for c in foresttiles
    tile = match(r"E\d\d\dN\d\d\dT3", c).match
    @show tile
    rqafiles = glob("*$(tile)*.zarr", rqadir)
    @show length(rqafiles)
    clustered = glob("*$(tile)*.nc", threshdir)
    @show length(clustered)
end

clustered = glob("*.nc", threshdir)
Raster(last(clustered))

