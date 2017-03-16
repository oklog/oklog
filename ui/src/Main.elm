port module Main exposing (main)

import Date exposing (fromTime)
import Dict exposing (Dict)
import Html
    exposing
        ( Html
        , button
        , div
        , footer
        , form
        , input
        , label
        , nav
        , option
        , program
        , section
        , select
        , span
        , strong
        , table
        , tbody
        , td
        , text
        , tr
        )
import Html.Attributes exposing (autofocus, class, for, id, placeholder, type_, value)
import Html.Events exposing (on, onClick, onInput, onSubmit, targetValue)
import Http exposing (Request, Response, emptyBody, expectStringResponse, request, send)
import Json.Decode as Json
import Task
import Time exposing (Time, every, hour, millisecond, minute)
import RFC3339


main : Program Never Model Msg
main =
    program
        { init = init
        , subscriptions = subscriptions
        , update = update
        , view = view
        }



-- MODEL


type alias Model =
    { isPlanned : Bool
    , lastPlan : Time
    , now : Time
    , query : Query
    , records : List Record
    , stats : Stats
    }


type alias Query =
    { regex : Bool
    , term : String
    , to : String
    , window : String
    }


type alias Record =
    { line : String
    , ulid : String
    }


type alias Stats =
    { errors : Int
    , maxDataSetSize : Int
    , nodesQueried : Int
    , segmentsQueried : Int
    }


init : ( Model, Cmd Msg )
init =
    ( Model False 0 0 initQuery [] initStats, Task.perform Tick Time.now )


initQuery : Query
initQuery =
    Query False "" defaultTo defaultWindow


initStats : Stats
initStats =
    Stats 0 0 0 0



-- UPDATE


type Msg
    = QueryFormSubmit
    | QueryTermUpdate String
    | QueryToggleRegex
    | QueryToUpdate String
    | QueryWindowUpdate String
    | StatsUpdate (Result Http.Error Stats)
    | StreamComplete String
    | StreamError String
    | StreamLines (List String)
    | Tick Time


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        QueryFormSubmit ->
            ( { model | records = [] }, getRecords model.now model.query )

        QueryTermUpdate term ->
            let
                query =
                    model.query
            in
                ( { model | isPlanned = False, query = { query | term = term } }, Cmd.none )

        QueryToggleRegex ->
            let
                query =
                    model.query
            in
                ( { model | query = { query | regex = not query.regex } }, Cmd.none )

        QueryToUpdate to ->
            let
                query =
                    model.query
            in
                ( { model | isPlanned = False, query = { query | to = to } }, Cmd.none )

        QueryWindowUpdate window ->
            let
                query =
                    model.query
            in
                ( { model | isPlanned = False, query = { query | window = window } }, Cmd.none )

        StatsUpdate (Ok stats) ->
            ( { model | isPlanned = True, stats = stats }, Cmd.none )

        StatsUpdate (Err _) ->
            ( model, Cmd.none )

        StreamComplete _ ->
            ( model, Cmd.none )

        StreamError _ ->
            ( model, Cmd.none )

        StreamLines lines ->
            let
                parseRecord line =
                    Record (String.dropLeft 27 line) (String.left 26 line)

                records =
                    List.map parseRecord lines

            in
                ( { model | records = (model.records ++ records) }, streamContinue "" )

        Tick now ->
            let
                timeHasPassed =
                    (abs (model.lastPlan - now)) >= 100

                termNotEmpty =
                    model.query.term /= ""

                shouldPlan =
                    (not model.isPlanned) && timeHasPassed && termNotEmpty
            in
                case shouldPlan of
                    True ->
                        ( { model | lastPlan = now, now = now }, getStats now model.query )

                    False ->
                        ( { model | now = now }, Cmd.none )



-- SUBSCRIPTION


subscriptions : Model -> Sub Msg
subscriptions model =
    Sub.batch
        [ every (100 * millisecond) Tick
        , streamComplete StreamComplete
        , streamError StreamError
        , streamLine StreamLines
        ]


-- PORTS

port streamContinue : String -> Cmd msg
port streamRequest : String -> Cmd msg

port streamComplete : ( String -> msg ) -> Sub msg
port streamError : ( String -> msg ) -> Sub msg
port streamLine : ( List String -> msg ) -> Sub msg

-- API


queryUrl : Time -> Query -> String
queryUrl now query =
    let
        from =
            Http.encodeUri (RFC3339.encode (fromTime (now - (windowDuration query.window))))

        to =
            Http.encodeUri (RFC3339.encode (fromTime now))

        parts =
           [ "/store/query?from="
           , from
           , "&to="
           , to
           , "&q="
           , query.term
           ]

    in
        if query.regex then
            String.concat (parts ++ [ "&regex" ])
        else
           String.concat parts


getRecords : Time -> Query -> Cmd Msg
getRecords now query =
    streamRequest (queryUrl now query)


getStats : Time -> Query -> Cmd Msg
getStats now query =
    Http.request
        { body = emptyBody
        , expect = expectStringResponse readStats
        , headers = []
        , method = "HEAD"
        , timeout = Nothing
        , url = queryUrl now query
        , withCredentials = False
        }
        |> send StatsUpdate


readStats : Response String -> Result String Stats
readStats response =
    let
        extractHeader =
            extractStatsHeader response.headers

        errors =
            extractHeader "X-Oklog-Error-Count"

        maxDataSet =
            extractHeader "X-Oklog-Max-Data-Set-Size"

        nodesQueried =
            extractHeader "X-Oklog-Nodes-Queried"

        segmentsQueried =
            extractHeader "X-Oklog-Segments-Queried"
    in
        Ok (Stats errors maxDataSet nodesQueried segmentsQueried)


extractStatsHeader : Dict String String -> String -> Int
extractStatsHeader headers header =
    case Dict.get header headers of
        Nothing ->
            0

        Just value ->
            Result.withDefault 0 (String.toInt value)



-- VIEW


view : Model -> Html Msg
view model =
    div [ class "page" ]
        [ nav []
            [ div [ class "container" ]
                [ formQuery model ]
            ]
        , section [ id "result" ]
            [ viewRecords model
            ]
        , footer []
            [ div [ class "container" ]
                []
              --[ viewDebug model
              --]
            ]
        ]


viewDebug : Model -> Html Msg
viewDebug model =
    let
        slimModel =
            { isPlanned = model.isPlanned
            , lastPlan = model.lastPlan
            , now = model.now
            , query = model.query
            , stats = model.stats
            }
    in
        div [ class "debug" ] [ text (toString slimModel) ]


viewMatchList : String -> String -> List Int -> List (Html Msg) -> List (Html Msg)
viewMatchList query line indexes elements =
    case List.head indexes of
        Nothing ->
            if String.length line == 0 then
                elements
            else
                elements ++ ([ span [] [ text line ] ])

        Just index ->
            let
                queryLen =
                    String.length query

                nonMatchElement =
                    span [] [ text (String.left index line) ]

                matchElement =
                    span [ class "highlight" ] [ text (String.left queryLen (String.dropLeft index line)) ]
            in
                viewMatchList query (String.dropLeft (index + queryLen) line) (List.drop 1 indexes) (elements ++ [ nonMatchElement, matchElement ])


viewPlan : Bool -> Stats -> Html Msg
viewPlan isPlanned stats =
    let
        nodeText =
            case stats.nodesQueried of
                1 ->
                    "1 node"

                _ ->
                    (toString stats.nodesQueried) ++ " nodes"

        segmentsText =
            case stats.segmentsQueried of
                1 ->
                    "1 segment"

                _ ->
                    (toString stats.segmentsQueried) ++ " segments"

        elements =
            case isPlanned of
                False ->
                    [ text "Your query hasn't been planned yet." ]

                True ->
                    [ span [] [ text "Your query could return " ]
                    , strong [] [ text (prettyPrintDataSet stats.maxDataSetSize) ]
                    , span [] [ text " reading " ]
                    , strong [] [ text segmentsText ]
                    , span [] [ text " on " ]
                    , strong [] [ text nodeText ]
                    ]
    in
        div [ class "plan" ] elements


viewRecord : String -> Int -> Record -> Html Msg
viewRecord query index record =
    let
        cellClass =
            if index % 2 == 0 then
                "even"
            else
                "odd"
    in
        tr [ class cellClass ]
            [ td [] (viewMatchList query record.line (String.indexes query record.line) [])
            ]


viewRecords : Model -> Html Msg
viewRecords { query, records } =
    table []
        [ tbody [] (List.map2 (viewRecord query.term) (List.range 0 (List.length records)) records)
        ]


viewResultInfo : Int -> Html Msg
viewResultInfo numRecords =
    let
        recordCount =
            if numRecords == 0 then
                span [] []
            else
                span []
                    [ text "Displaying "
                    , strong [] [ text ((toString numRecords) ++ " records") ]
                    ]
    in
        div [ class "result-info" ] [ recordCount ]


formQuery : Model -> Html Msg
formQuery model =
    form [ id "query", onSubmit QueryFormSubmit ]
        [ div [ class "row" ]
            [ formElementQuery model.query.term
            ]
        , div [ class "row" ]
            [ formElementRange
            , div [ class "regex" ]
                [ input [ id "regex", onClick QueryToggleRegex, type_ "checkbox" ] []
                , label [ for "regex" ] [ text "as regex" ]
                ]
            , button [] [ text "Go" ]
            ]
        , div [ class "row" ]
            [ viewPlan model.isPlanned model.stats
            , viewResultInfo (List.length model.records)
            ]
        ]


formElementRange : Html Msg
formElementRange =
    div [ class "range" ]
        [ formElementWindow
        , strong [] [ text "-" ]
        , formElementTo
        ]


formElementWindow : Html Msg
formElementWindow =
    select [ on "change" (Json.map QueryWindowUpdate targetValue) ]
        [ option [ value "5m" ] [ text "5m ago" ]
        , option [ value "15m" ] [ text "15m ago" ]
        , option [ value "1h" ] [ text "1 hour ago" ]
        , option [ value "12h" ] [ text "12 hours ago" ]
        , option [ value "1d" ] [ text "1 day ago" ]
        , option [ value "3d" ] [ text "3 days ago" ]
        , option [ value "7d" ] [ text "7 days ago" ]
        ]


formElementTo : Html Msg
formElementTo =
    select [ on "change" (Json.map QueryToUpdate targetValue) ]
        [ option [ value "0" ] [ text "now" ]
        ]


formElementQuery : String -> Html Msg
formElementQuery query =
    div [ class "term" ]
        [ input
            [ autofocus True
            , onInput QueryTermUpdate
            , placeholder "Enter query"
            , value query
            ]
            []
        ]



-- CONSTANTS


day : Time
day =
    24 * hour


defaultTo : String
defaultTo =
    "now"


defaultWindow : String
defaultWindow =
    "1h"

windowDuration : String -> Time
windowDuration window =
    case window of
        "5m" ->
            5 * minute

        "15m" ->
            15 * minute

        "12h" ->
            12 * hour

        "1d" ->
            day

        "3d" ->
            3 * day

        "7d" ->
            7 * day

        _ ->
            hour



-- HELPER


prettyPrintDataSet : Int -> String
prettyPrintDataSet size =
    if size > 1000000000 then
        toString (size // 1000000000) ++ "GB"
    else if size > 1000000 then
        toString (size // 1000000) ++ "MB"
    else if size > 1000 then
        toString (size // 1000) ++ "kB"
    else
        (toString size) ++ "Bytes"
