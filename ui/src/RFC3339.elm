module RFC3339
    exposing
        ( decode
        , empty
        , encode
        )

import Date exposing (Date)
import Native.RFC3339
import Result


decode : String -> Result String Date
decode =
    Native.RFC3339.decode


encode : Date -> String
encode =
    Native.RFC3339.encode


empty : Date
empty =
    Native.RFC3339.empty
