// Learn more about F# at http://fsharp.net
// See the 'F# Tutorial' project for more help.
open Cyclic3.Alpha
let help() = ()
[<EntryPoint>]
let main argv = 
    let args = argv |> Seq.pairwise |> Map.ofSeq
    if Array.contains "-h" argv || Array.contains "--help" argv || Array.contains "/?" argv then help();exit 0
    args.[0]
    0 // return an integer exit code
