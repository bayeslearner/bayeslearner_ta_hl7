[hl7_modular_input://default]

title = <value>
* The title of the input

port = <value>
* The TCP port to listen at

output_kvp = <value>
raw HL7 message (filtered) or transformed key-value pairs

remove_phi = <value>
whether or not to remove common PHI segments

fields_to_remove = <value>
segments or fields to further remove from output, separated with ','.  For example,  pid, acc.acc_9, zxx  etc.

max_long_segment = <value>
segments longer than this (in bytes) will be removed

