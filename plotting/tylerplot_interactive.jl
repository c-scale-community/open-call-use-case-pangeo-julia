using Tyler
using Rasters
using WGLMakie
using Statistics
using PyramidScheme
using TileProviders
using Extents
using Glob
using NCDatasets

indir = "data/germany_all_years/forestmasked_all/forestmasked_all_webmercator/webmercator_"
yeardirs = Dict(y=>indir * string(y) for y in 2016:2021)

colordict = Dict(
    2016 => :red,
    2017 => :orange,
    2018 => :purple,
    2019 => :indigo,
    2020 => :firebrick,
    2021 => :orangered
)


fig = Figure()
togglegrid = GridLayout(fig[1,2], tellheight=false)
toggles = Dict(y => Toggle(togglegrid[i,1], active=true, tellheight=false,buttoncolor=colordict[y]) for (i,y) in enumerate(2016:2021))
labels = [Label(togglegrid[i,2], text=string(y)) for (i,y) in enumerate(2016:2021)]
ax = Axis(fig[1,1], title="Recurrence Quantification Analysis")

provider = TileProviders.GeoportailFrance(:orthos, apikey="choisirgeoportail")
#provider = TileProviders.CartoDB(:DarkMatter)
ext = Extent(X = (382992.3362567568, 2.0289240555728525e6), Y = (5.7789077698318e6, 7.42264830466397e6))
m = Tyler.Map(ext, Tyler.MapTiles.WebMercator(); provider, figure=fig, axis=ax, max_zoom=12)
y,path = first(yeardirs)
lazy=true
for (y, path) in yeardirs
    ncpaths = glob("*.nc", path)
    
    #cubes = Cube.(tilepaths)
    allpyramids = []

    for p in ncpaths
        pyrfolder = splitext(p)[1] * "_pyramids"
        pyrfiles = readdir(pyrfolder, join=true)
        push!(allpyramids, [Raster(p; lazy), Raster.(pyrfiles;lazy)...])
    end
    colormap = resample_cmap([:transparent, colordict[y], colordict[y]], 3) .* 10
    #colormap = (:reds, 1)
    hmaps = PyramidScheme.plotpyramids!.((m.axis,), allpyramids; colormap=colormap, colorrange=(0,1))
    #Makie.translate!.(hmaps, 0,0,100)
    connect!.(getproperty.(hmaps, :visible), (toggles[y].active,))
end
#provider = TileProviders.GeoportailFrance(:orthos, apikey="choisirgeoportail")
#ext = Extents.union(cubes...)


#hmaps = PyramidScheme.plotpyramids!.((m.axis,), allpyramids, colormap=(:reds,1), colorrange=(0,1))
#connect!.(getproperty.(hmaps, :visible), (toggle2018.active,))

