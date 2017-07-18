#' \code{parcels} package
#'
#' parcels
#'
#' See the Vignette on
#'
#' @docType package
#' @name parcels
#' @importFrom dplyr %>% select
#' @importFrom data.table :=
NULL

## quiets concerns of R CMD check re: the .'s that appear in pipelines
if(getRversion() >= "2.15.1")  utils::globalVariables(c("."))

#' @title address
#'
#' @description Creates parcels.address file
#' @param data.path character vector
#' @param fresh logical TRUE for fresh run
#' @keywords parcels, clean
#' @export
#' @import stringr
#'     data.table
#'     stringdist
#'     methods.string
#'     foreign

address <- function(data.path, fresh=FALSE){
  raw.path <- paste0(data.path, '/raw/')
  clean.path <- paste0(data.path, '/clean/address/')
  raw <- list(arapahoe = paste0(raw.path,'Arapahoe County/AssessorParcels_SHAPE_SP/AssessorParcels_SP.dbf'),
              boulder = paste0(raw.path, 'Boulder County/Owner_Address.csv'),
              broomfield = paste0(raw.path, 'Broomfield County/Addresses.csv'),
              clearCreek = paste0(raw.path, 'Clear Creek County/address/addr_08_01_2016.dbf'),
              denver = paste0(raw.path, '/Denver County/parcels/parcels.csv'),
              douglas = paste0(raw.path, 'Douglas County/Property_Location_Data.csv'),
              gilpin =  paste0(raw.path, 'Gilpin County/AddressPoints/AddressPoints.dbf'))
  clean <- list(address = paste0(clean.path, 'parcels.address.rdata'),
                address.list = paste0(clean.path, 'parcels.address.list.rdata'))
  l.path <- list(raw=raw,clean=clean)            
  # ROxygen initialize
  location.id <- NULL; street <- NULL; cityStateZip <- NULL; location.source <- NULL
  street.num <- NULL; street.direction.prefix <- NULL; street.body <- NULL
  street.unit <- NULL; city.raw <- NULL; street.type <- NULL
  if (file.exists(l.path$clean$address) & !fresh) {
    load(l.path$clean$address)
  } else {
    if (file.exists(l.path$clean$address.list) & !fresh){
      load(l.path$clean$address.list)
    } else {
      address.list <- list()
      # arapahoe
      parcels.arapahoe <- read.dbf(l.path$raw$arapahoe)
      setnames(parcels.arapahoe, names(parcels.arapahoe), underscore(names(parcels.arapahoe)))
      oldNames <- c('parcelId', 'situsAddr', 'situsCity')
      newNames <- c('location.id', 'street', 'cityStateZip')
      setnames(parcels.arapahoe, oldNames, newNames)
      parcels.arapahoe <- as.data.table(parcels.arapahoe)
      parcels.arapahoe <- parcels.arapahoe[, (newNames), with=FALSE]
      parcels.arapahoe <- parcels.arapahoe[, location.id := as.character(location.id)]
      parcels.arapahoe <- parcels.arapahoe[, street := as.character(street)]
      parcels.arapahoe <- parcels.arapahoe[, cityStateZip := as.character(cityStateZip)]
      parcels.arapahoe <- parcels.arapahoe[,location.source :='arapahoe']
      parcels.address.list$arapahoe <- parcels.arapahoe
      
      # boulder
      parcels.boulder.location <- l.path$raw$boulder
      parcels.boulder <- fread(parcels.boulder.location, colClasses = 'character')
      setnames(parcels.boulder, names(parcels.boulder), underscore(names(parcels.boulder)))
      oldNames <- c('folio', 'strNum', 'str', 'strPfx',
                    'strSfx', 'strUnit',
                    'city','mailingzip')
      newNames <- c('location.id', 'street.num', 'street.body',
                    'street.direction.prefix', 'street.type', 'street.unit',
                    'city.raw', 'zip')
      setnames(parcels.boulder, oldNames, newNames)
      
      parcels.boulder <- parcels.boulder[, street :=str_trim(paste(street.num,
                                                                   street.direction.prefix,
                                                                   street.body, street.type, street.unit))]
      parcels.boulder <- parcels.boulder[,.(location.id, street,
                                            cityStateZip = paste(city.raw, 'CO', zip ,sep=', '))]
      parcels.boulder <- parcels.boulder[, location.source:='boulder']
      parcels.address.list$boulder <- parcels.boulder
      
      # broomfield
      parcels.broomfield.location <- l.path$raw$broomfield
      parcels.broomfield <-  fread(parcels.broomfield.location, colClasses = 'character')
      setnames(parcels.broomfield, names(parcels.broomfield),
               methods.string::underscore(names(parcels.broomfield)))
      oldNames <- c('objectid', 'fullAddress', 'city', 'state', 'zipcode')
      newNames <- c('location.id', 'street', 'city', 'state', 'zip')
      setnames(parcels.broomfield, oldNames, newNames)
      parcels.broomfield <- parcels.broomfield[, (newNames), with=FALSE]
      parcels.broomfield <- parcels.broomfield[, cityStateZip := paste(city, state, zip, sep=',')]
      parcels.broomfield <- parcels.broomfield[, .(location.id, street, cityStateZip, location.source='broomfield')]
      parcels.address.list$broomfield <- parcels.broomfield
      
      # clearCreek
      parcels.clearCreek.location <- l.path$raw$clearCreek
      parcels.clearCreek <- as.data.table(read.dbf(parcels.clearCreek.location))
      setnames(parcels.clearCreek, names(parcels.clearCreek), 
               methods.string::underscore(names(parcels.clearCreek)))
      oldNames <- c('pin', 'fullAddr', 'city', 'zipCode')
      newNames <- c('location.id', 'street', 'city', 'zip')
      setnames(parcels.clearCreek, oldNames, newNames)
      parcels.clearCreek <- parcels.clearCreek[,(newNames), with=FALSE]
      parcels.clearCreek$location.id <- as.character(parcels.clearCreek$location.id)
      parcels.clearCreek$street <- as.character(parcels.clearCreek$street)
      city <- NULL; state <- NULL; zip <- NULL; cityStateZip <- NULL
      parcels.clearCreek <- parcels.clearCreek[, cityStateZip := paste(city, 'CO', zip, sep=', ')]
      cols <- c('location.id', 'street', 'cityStateZip')
      parcels.clearCreek <- parcels.clearCreek[, (cols), with =FALSE]
      parcels.clearCreek$location.source <- 'clearCreek'
      parcels.address.list$clearCreek <- parcels.clearCreek
      
      ## denver
      parcels.denver <- fread(l.path$raw$denver)
      setnames(parcels.denver, names(parcels.denver), 
               methods.string::underscore(names(parcels.denver)))
      oldNames <- c('pin',
                    'situsAddressLine1',
                    'situsCity', 'situsState', 'situsZip')
      newNames <- c('location.id',
                    'street',
                    'city', 'state', 'zip')
      setnames(parcels.denver, oldNames, newNames)
      parcels.denver <- parcels.denver[, (newNames), with=FALSE]
      parcels.denver <- parcels.denver[, cityStateZip := paste(city, state, zip, sep=', ')]
      parcels.denver <- parcels.denver[, .(location.id, street, cityStateZip, location.source='denver')]
      parcels.address.list$denver <- parcels.denver
      
      ## douglas
      parcels.douglas <-  fread(l.path$raw$douglas)
      setnames(parcels.douglas, names(parcels.douglas),
               methods.string::underscore(names(parcels.douglas)))
      oldNames <- c('stateParcelNo', 'addressNumber', 'preDirectionCode', 'streetName', 
                    'streetTypeCode', 'unitNo', 'cityName', 'locationZipCode')
      newNames <- c('location.id', 'street.num', 'street.direction.prefix', 'street.body',
                    'street.type', 'street.unit', 'city','zip')
      setnames(parcels.douglas, oldNames, newNames)
      parcels.douglas <- parcels.douglas[, street := str_trim(paste(street.num, street.direction.prefix,
                                                                    street.body, street.type, street.unit))]
      parcels.douglas <- parcels.douglas[,.(location.id, street, cityStateZip=paste(city, 'CO', zip, sep=','))]
      parcels.address.list$douglas <- parcels.douglas[, location.source:='douglas']
      
      ## gilpin   
      parcels.gilpin <-  as.data.table(read.dbf(l.path$raw$gilpin))
      setnames(parcels.gilpin, names(parcels.gilpin),
               methods.string::underscore(names(parcels.gilpin)))
      oldNames <- c('siteaddid', 'fulladdr', 'city','zipcode')
      newNames <- c('location.id', 'street', 'city', 'zip')
      setnames(parcels.gilpin, oldNames, newNames)
      parcels.gilpin <- parcels.gilpin[,.(location.id,  street, cityStateZip=paste(city, 'CO',  zip, sep=', '))]
      parcels.gilpin <- parcels.gilpin[, street:=as.character(street)]
      parcels.gilpin <- parcels.gilpin[, location.id:=as.character(location.id)]
      parcels.address.list$gilpin <- parcels.gilpin[, location.source:='gilpin']
      save(parcels.address.list, file= l.path$clean$address.list)
    }
    # Combine all parcel information
    parcels.address <- rbindlist(parcels.address.list, use.names=TRUE)
    parcels.address <- parcels.address[!(is.na(street) | street =='' | is.na(location.id) |  location.id == '')]
    study.cities <- methods.string::study.cities
    parcels.address <- methods.string::explode.address(parcels.address, study.cities)
    parcels.address <- parcels.address[location.id == 161869536, street.unit:='']
    # if (file.exists(parcels.address.geocode.location)){
    #  load(parcels.address.geocode.location)
    #  parcels.address <- rbindlist(list(parcels.address, parcels.address.geocode), use.names=TRUE, fill=TRUE)
    # }
    parcels.address$location.type <- 'parcels'
    # # Add areas in square feet
    # shapes.parcels <- funShapes.parcels()
    # parcels.area <- as.data.table(shapes.parcels@data)
    # setnames(parcels.area, 'parcel.id', 'location.id')
    # setkey(parcels.area, location.id)
    # setkey(parcels.address, location.id)
    # parcels.address <- parcels.area[parcels.address]
    save(parcels.address, file=l.path$clean$address)
  }
  return(parcels.address)
}
#' @title shapes
#'
#' @description Creates parcels.address file
#' @param data.path character vector
#' @param fresh logical TRUE for fresh run
#' @keywords parcels, clean
#' @export
#' @import stringr
#'     data.table
#'     stringdist
#'     methods.string
#'     foreign
#'     rgeos
#'     raster
#'     rgdal
#'     methods.shapes
#' @importFrom dplyr select
shapes <- function(data.path, fresh=FALSE){
  raw.path <- paste0(data.path, '/raw/')
  clean.path <- paste0(data.path, '/clean/shapes/')
  raw <- list(arapahoe = paste0(raw.path,'Arapahoe County/AssessorParcels_SHAPE_SP/'),
              boulder = paste0(raw.path, 'Boulder County/Parcels'),
              broomfield = paste0(raw.path, 'Broomfield County/Parcels'),
              clearCreek = paste0(raw.path, 'Clear Creek County/parcels'),
              denver = paste0(raw.path, '/Denver County/parcels'),
              douglas = paste0(raw.path, 'Douglas County/Parcel'),
              gilpin =  paste0(raw.path, 'Gilpin County/Parcels'))
  clean <- list(shapes = paste0(clean.path, 'shapes.parcels.rdata'),
                shapes.list = paste0(clean.path, 'shapes.pardels.list.rdata'),
                arapahoe = paste0(clean.path,'arapahoe.rdata'),
                boulder = paste0(clean.path, 'boulder.rdata'),
                broomfield = paste0(clean.path, 'broomfield.rdata'),
                clearCreek = paste0(clean.path, 'clearCreek.rdata'),
                denver = paste0(clean.path, 'denver.rdata'),
                douglas = paste0(clean.path, 'douglas.rdata'),
                gilpin =  paste0(clean.path, 'gilpin.rdata'))
  l.path <- list(raw=raw,clean=clean)           
  l.path$raw <- lapply(l.path$raw, path.expand) 
  proj4string <- NULL; spTransform <- NULL; CRS <- NULL; parcel.id <- NULL
  if (file.exists(l.path$clean$shapes) & !fresh){
    print('loading shapes.parcels')
    load(l.path$clean$shapes)
  } else {
    print('building shapes.parcels')
    shapes.parcels.list <- list()
    shape.dir <- '../RawData/Parcels/Clean/'
    proj.env <- methods.shapes::shapes.proj.env
    # Arapahoe
    if (file.exists(l.path$clean$arapahoe)){
      load(l.path$clean$arapahoe)
    } else { 
      parcels.arapahoe.shapes <- readOGR(dsn = l.path$raw$arapahoe,
                                         layer = 'AssessorParcels_SP', verbose = FALSE)
      # Clean up dbf and subset
      parcels.arapahoe.names <- names(parcels.arapahoe.shapes@data)
      parcels.arapahoe.shapes@data <- setnames(parcels.arapahoe.shapes@data,
                                               parcels.arapahoe.names, methods.string::underscore(parcels.arapahoe.names)) 
      setnames(parcels.arapahoe.shapes@data, 'parcelId', 'parcel.id')
      parcels.arapahoe.shapes@data$parcel.id <- as.character(parcels.arapahoe.shapes@data$parcel.id)
      parcels.nShapes.orig <- nrow(parcels.arapahoe.shapes@data)
      # Project shp
      
      parcels.arapahoe.shapes.proj.orig <- proj4string(parcels.arapahoe.shapes) # Original projections
      parcels.arapahoe.shapes.proj.new <- proj.env
      
      parcels.arapahoe.shapes <- parcels.arapahoe.shapes %>%
        spTransform(CRS(parcels.arapahoe.shapes.proj.new))
      
      parcels.arapahoe.shapes@data <- dplyr::select(parcels.arapahoe.shapes@data, parcel.id)
      save(parcels.arapahoe.shapes, file = l.path$clean$arapahoe)
      
      shapes.parcels$arapahoe <- parcels.arapahoe.shapes
    }
    shapes.parcels.list$arapahoe <- parcels.arapahoe.shapes
    
    # Boulder
    if (file.exists(l.path$clean$boulder)){
      load(l.path$clean$boulder)
    } else { 
      parcels.boulder.shapes <- readOGR(dsn = l.path$raw$boulder,
                                        layer = 'Parcels', verbose = FALSE)
      # Clean up dbf and subset
      parcels.boulder.names <- names(parcels.boulder.shapes@data)
      parcels.boulder.shapes@data <- setnames(parcels.boulder.shapes@data,
                                              parcels.boulder.names, methods.string::underscore(parcels.boulder.names)) 
      setnames(parcels.boulder.shapes@data, 'parcelNo', 'parcel.id')
      parcels.boulder.shapes@data$parcel.id <- as.character(parcels.boulder.shapes@data$parcel.id)
      parcels.nShapes.orig <- nrow(parcels.boulder.shapes@data)
      # Project shp
      parcels.boulder.shapes.proj.orig <- proj4string(parcels.boulder.shapes) # Original projections
      parcels.boulder.shapes.proj.new <- proj.env
      parcels.boulder.shapes <- parcels.boulder.shapes %>%
        spTransform(CRS(parcels.boulder.shapes.proj.new))
      parcels.boulder.shapes@data <- dplyr::select(parcels.boulder.shapes@data, parcel.id)
      save(parcels.boulder.shapes, file = l.path$clean$boulder)
    }
    shapes.parcels.list$boulder <- parcels.boulder.shapes
    
    # Broomfield
    if (file.exists(l.path$clean$broomfield)){
      load(l.path$clean$broomfield)
    } else { 
      parcels.broomfield.shapes <- readOGR(dsn = l.path$raw$broomfield,
                                           layer = 'Parcels', verbose = FALSE)
      # Clean up dbf and subset
      parcels.broomfield.names <- names(parcels.broomfield.shapes@data)
      parcels.broomfield.shapes@data <- setnames(parcels.broomfield.shapes@data,
                                                 parcels.broomfield.names, methods.string::underscore(parcels.broomfield.names)) 
      setnames(parcels.broomfield.shapes@data, 'parcelnumb', 'parcel.id')
      parcels.broomfield.shapes@data$parcel.id <- as.character(parcels.broomfield.shapes@data$parcel.id)
      parcels.nShapes.orig <- nrow(parcels.broomfield.shapes@data)
      # Project shp
      parcels.broomfield.shapes.proj.orig <- proj4string(parcels.broomfield.shapes) # Original projections
      parcels.broomfield.shapes.proj.new <- proj.env
      parcels.broomfield.shapes <- parcels.broomfield.shapes %>%
        spTransform(CRS(parcels.broomfield.shapes.proj.new))
      parcels.broomfield.shapes@data <- dplyr::select(parcels.broomfield.shapes@data, parcel.id)
      save(parcels.broomfield.shapes, file = l.path$clean$broomfield)
    }
    shapes.parcels.list$broomfield <- parcels.broomfield.shapes
    
    # Clear Creek
    if (file.exists(l.path$clean$clearCreek)){
      load(l.path$clean$clearCreek)
    } else { 
      parcels.clearCreek.shapes <- readOGR(dsn = l.path$raw$clearCreek,
                                           layer = 'parcels_08_01_2016', verbose = FALSE)
      # Clean up dbf and subset
      parcels.clearCreek.names <- names(parcels.clearCreek.shapes@data)
      parcels.clearCreek.shapes@data <- setnames(parcels.clearCreek.shapes@data,
                                                 parcels.clearCreek.names, methods.string::underscore(parcels.clearCreek.names)) 
      setnames(parcels.clearCreek.shapes@data, 'pin', 'parcel.id')
      parcels.clearCreek.shapes@data$parcel.id <- as.character(parcels.clearCreek.shapes@data$parcel.id)
      parcels.nShapes.orig <- nrow(parcels.clearCreek.shapes@data)
      # Project shp
      parcels.clearCreek.shapes.proj.orig <- proj4string(parcels.clearCreek.shapes) # Original projections
      parcels.clearCreek.shapes.proj.new <- proj.env
      parcels.clearCreek.shapes <- parcels.clearCreek.shapes %>%
        spTransform(CRS(parcels.clearCreek.shapes.proj.new))
      parcels.clearCreek.shapes@data <- dplyr::select(parcels.clearCreek.shapes@data, parcel.id)
      save(parcels.clearCreek.shapes, file = l.path$clean$clearCreek)
    }
    shapes.parcels.list$clearCreek <- parcels.clearCreek.shapes
    
    # Denver
    
    if (file.exists(l.path$clean$denver)){
      load(l.path$clean$denver)
    } else { 
      parcels.denver.shapes <- readOGR(dsn = l.path$raw$denver,
                                       layer = 'parcels', verbose = FALSE)
      # Clean up dbf and subset
      parcels.denver.name <- names(parcels.denver.shapes@data)
      parcels.denver.shapes@data <- setnames(parcels.denver.shapes@data,
                                             parcels.denver.name, methods.string::underscore(parcels.denver.name)) 
      parcels.denver.shapes@data$actZone <- as.character(parcels.denver.shapes@data$actZone)
      parcels.denver.shapes@data$schednum <- as.character(parcels.denver.shapes@data$schednum)
      parcels.nShapes.orig <- nrow(parcels.denver.shapes)
      # Project shp
      parcels.denver.shapes.proj.orig <- proj4string(parcels.denver.shapes) # Original projections
      projUrl <- 'http://spatialreference.org/ref/epsg/2232/proj4js/'
      parcels.denver.shapes.proj.new <- proj.env
      parcels.denver.shapes <- parcels.denver.shapes %>%
        spTransform(CRS(parcels.denver.shapes.proj.new))
      setnames(parcels.denver.shapes@data, 'pin', 'parcel.id')
      parcels.denver.shapes@data <- dplyr::select(parcels.denver.shapes@data, parcel.id)
      save(parcels.denver.shapes, file = l.path$clean$denver)
    }
    shapes.parcels.list$denver <- parcels.denver.shapes
    
    # Douglas
    
    if (file.exists(l.path$clean$douglas)){
      load(l.path$clean$douglas)
    } else { 
      parcels.douglas.shapes <- readOGR(dsn = l.path$raw$douglas,
                                        layer = 'Parcel', verbose = FALSE)
      # Clean up dbf and subset
      parcels.douglas.names <- names(parcels.douglas.shapes@data)
      parcels.douglas.shapes@data <- setnames(parcels.douglas.shapes@data,
                                              parcels.douglas.names, methods.string::underscore(parcels.douglas.names)) 
      setnames(parcels.douglas.shapes@data, 'parcelspn', 'parcel.id')
      parcels.douglas.shapes@data$parcel.id <- as.character(parcels.douglas.shapes@data$parcel.id)
      parcels.nShapes.orig <- nrow(parcels.douglas.shapes@data)
      # Project shp
      parcels.douglas.shapes.proj.orig <- proj4string(parcels.douglas.shapes) # Original projections
      parcels.douglas.shapes.proj.new <- proj.env
      parcels.douglas.shapes <- parcels.douglas.shapes %>%
        spTransform(CRS(parcels.douglas.shapes.proj.new))
      parcels.douglas.shapes@data <- dplyr::select(parcels.douglas.shapes@data, parcel.id)
      save(parcels.douglas.shapes, file = l.path$clean$douglas)
    }
    shapes.parcels.list$douglas <- parcels.douglas.shapes
    
    # Gilpin
    if (file.exists(l.path$clean$gilpin)){
      load(l.path$clean$gilpin)
    } else { 
      parcels.gilpin.shapes <- readOGR(dsn = l.path$raw$gilpin,
                                       layer = 'Parcels', verbose = FALSE)
      # Clean up dbf and subset
      parcels.gilpin.names <- names(parcels.gilpin.shapes@data)
      parcels.gilpin.shapes@data <- setnames(parcels.gilpin.shapes@data,
                                             parcels.gilpin.names, methods.string::underscore(parcels.gilpin.names)) 
      setnames(parcels.gilpin.shapes@data, 'parcelid', 'parcel.id')
      parcels.gilpin.shapes@data$parcel.id <- as.character(parcels.gilpin.shapes@data$parcel.id)
      parcels.gilpin.shapes@data <- dplyr::select(parcels.gilpin.shapes@data, parcel.id)
      parcels.nShapes.orig <- nrow(parcels.gilpin.shapes@data)
      # Project shp
      parcels.gilpin.shapes.proj.new <- proj.env
      parcels.gilpin.shapes <- parcels.gilpin.shapes %>%
        spTransform(CRS(parcels.gilpin.shapes.proj.new))
      parcels.gilpin.shapes@data <- dplyr::select(parcels.gilpin.shapes@data, parcel.id)
      save(parcels.gilpin.shapes, file = l.path$clean$gilpin)
    }
    shapes.parcels.list$gilpin<- parcels.gilpin.shapes
    
    save(shapes.parcels.list, file=l.path$clean$shapes.list)
    # Combine into one (have to use rbind individually)
    shapes.parcels <- shapes.parcels.list[[1]]
    for (iShape in 2:length(shapes.parcels.list)){
      print(names(shapes.parcels.list)[iShape])
      parcel.shape <- shapes.parcels.list[[iShape]]
      shapes.parcels <-rbind(shapes.parcels, parcel.shape, makeUniqueIDs=TRUE)
    }
    
    # Include square feet
    shapes.parcels@data$parcels.area <- gArea(shapes.parcels, byid=TRUE)
    writeOGR(shapes.parcels, ".", "../CleanData/EsriShapes/parcels", driver="ESRI Shapefile")
    save(shapes.parcels, file=l.path$clean$shapes)
  }
  return(shapes.parcels)
}
