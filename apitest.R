



###==========================================

## TOP MATTER

# Libraries.
library(rjson)
library(magrittr)
library(httr)
library(RCurl)
library(rvest)
library(data.table)

# Access URL properly and retrieve JSON.
retrieved_url <- content( GET( string ) , 
                          as ="text" ,
                          type = "text/html" )


## PREPARE COUNTRY LISTS 
reporter <- jsonlite::fromJSON( "https://comtrade.un.org/data/cache/reporterAreas.json" )
partner <- jsonlite::fromJSON( "https://comtrade.un.org/data/cache/partnerAreas.json" )

# Turn into dataframe
reporter <- as.data.frame( reporter )
colnames( reporter ) <- names( reporter )
reporter

partner <- as.data.frame( partner )
colnames( partner ) <- names( partner )
partner

reporter %>%
  .$results.id %>%
  as.vector( . ) -> reporters_slist

partner %>%
  .$results.id %>%
  as.vector( . ) -> partners_slist


# A directory where we save things.
"" -> project_directory


###==========================================


## PROGRAM 1. GET DATA FROM THE UN COMTRADE API.

# First deal with HTTPS.
httr::set_config( config( ssl_verifypeer = 0L ) )

# Define UN COMTRADE RETRIEVAL FUNCTION.
get.Comtrade <- function( url="https://comtrade.un.org/api/get?"
                          , maxrec=50000
                          , type="C"
                          , freq="A"
                          , px="HS"
                          , ps="now"
                          , r
                          , p
                          , rg="all"
                          , cc="280530"
                          , fmt="json" )
  
{
  
  # Tell us where we are.
  paste( "Querying reporting country", r , 
         "trade with" , p , 
         sep = " " ) %>% 
    print( . )
  
  # The URL string constructed from function arguments.
  string <- paste(url
                  ,"max=",maxrec,"&" #maximum no. of records returned
                  ,"type=",type,"&" #type of trade (c=commodities)
                  ,"freq=",freq,"&" #frequency
                  ,"px=",px,"&" #classification
                  ,"ps=",ps,"&" #time period
                  ,"r=",r,"&" #reporting area
                  ,"p=",p,"&" #partner country
                  ,"rg=",rg,"&" #trade flow
                  ,"cc=",cc,"&" #classification code
                  ,"fmt=",fmt #Format
                  ,sep = ""
  )
  
  # How to process .csv OR the .json data.
  if( fmt == "csv" ) {
    
    # Read.csv.
    data <- read.csv( text = content( GET(string), 
                                      as="text" ,  
                                      type = "text/csv" , 
                                      header = TRUE ))
    
    # Make dataset into datatable. 
    save.Comtrade( data , px , ps , r , p ) 
    
  } else {
    
    if(fmt == "json" ) {
      
      # Access URL properly and retrieve JSON.
      retrieved_url <- content( GET( string ) , 
                                as ="text" ,
                                type = "text/html" )
      raw.data <- jsonlite::fromJSON( retrieved_url )
      
      # Turn into dataframe
      data <- as.data.frame( raw.data )
      colnames( data ) <- names( data )
      
      # Make dataset into datatable. 
      save.Comtrade( data , px , ps , r , p ) 
      
    }
  }
}


###==========================================


## PROGRAM 2. SAVE PREPARED DATA.FRAME AS CSV.

# Path for raw UN comtrade queries.
working_path= ""
file.path( working_path , 
           "data" , 
           "uncomtradequeries" ) ->  rawcomtrade_path


## ... Takes argumenents from main function get.Comtrade
save.Comtrade <- function( dataset_argument , px , ps , r , p ){
  
  # Prepare file name, pass to fast writing CSV method.
  paste( "trade" , 
         px , 
         ps, 
         r , 
         p , 
         "raw.csv" , 
         sep = "_" ) -> filename
  
  # Tell us where we are.
  paste( "Saving dataset for reporting country", r , 
         "trade with" , p , 
         sep = " " ) %>% 
    print( . )
  
  # Concatenate the project directory and filename.
  filename %>%
    file.path( project_directory , . ) %>%
    
    # Save the dataset as a CSV.
    write.csv( dataset_argument , file = . )  
  
}

### ===============================

## EXECUTE QUERY FOR ALL COUNTRY PAIRS.

# Build array (actually, data.frame) of arguments for function:
argument_array <- expand.grid( partner_arguments = partners_slist , 
                               reporting_arguments = reporters_slist )

# Show arguments array:
argument_array


# Take multiple arguments and apply the function.
mapply( get.Comtrade ,
        r = argument_array$reporting_arguments ,
        p = argument_array$partner_arguments , 
        MoreArgs = list( ps = "all" , 
                         fmt = "csv" ,
                         rg = "1" , 
                         px = "h0" , 
                         cc = "all") ) 


## MAKE DATASET OUT OF ALL SAVED FILES

## Make list of files we saved locally to the project path.
csvfile_list <- list.files( path = project_directory ,
                            pattern = "trade_h0.*.csv" , 
                            all.files = FALSE ,
                            full.names = TRUE , 
                            recursive = FALSE )

# Lapply the rest of the files to the main.dt file 1.
lapply( csvfile_list , fread , sep = "," ) %>% 
  rbindlist( . , fill = TRUE ) -> main.dt


# Save the stacked CSV.
write.csv( main.dt , 
           file = file.path( project_directory , 
                             "thebigdataset.csv" ) )
