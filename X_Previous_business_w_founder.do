clear all
set more off

cap log using ${LOG}X_Previous_business_w_founder, replace


*******************************************
* Possible CEO/Founder pairs win frame 	
*******************************************	

use ${OUTPUT}Data_product_2.dta, clear

ren person_id official_id 
joinby frame_id using ${OUTPUT}Data_product_4.dta
ren person_id CEO_id
ren official_id person_id

sort frame_id CEO_id person_id  start_date
keep frame_id person_id CEO_id 

order frame_id CEO_id person_id  

save ${OUTPUT}Possible_CEO_founder_connections.dta, replace

*******************************************
* Non-CEO workers 	
*******************************************	

use ${OUTPUT}Appended_mandb_owner.dta, clear

merge m:1 frame_id person_id start_date  using ${OUTPUT}Data_product_4.dta, gen(merging_CEOs_back)

g byte CEO_in_DP4 = (merging_CEOs_back == 3)

drop if CEO_in_DP4 == 1
ren person_id official_id 
ren start_date official_start_date
ren end_date official_end_date

keep frame_id official_id official_start_date official_end_date

save ${OUTPUT}Only_non_CEOs.dta, replace

*******************************************
* CEO connections with earliest dates 	
*******************************************	

use ${OUTPUT}Data_product_3.dta, clear

drop type_of_ceo

joinby frame_id using ${OUTPUT}Only_non_CEOs.dta

*CEO spell has to have an overlap with the official spell
keep if (official_end_date > start_date & official_start_date < end_date)

egen connection_starts = rowmax(official_start_date start_date)
format connection_starts %td

ren person_id CEO_id
ren official_id person_id

collapse (min) connection_starts, by(CEO_id person_id)

save ${OUTPUT}CEOs_connections.dta, replace

*******************************************
* Keeping connections where one is a founder 	
*******************************************

use ${OUTPUT}Possible_CEO_founder_connections.dta, clear

merge m:1 CEO_id person_id using ${OUTPUT}CEOs_connections.dta, gen(appear_w_founder)
keep if appear_w_founder == 3

collapse (min) connection_starts ,by(frame_id CEO_id)

ren CEO_id person_id

save ${OUTPUT}Earliest_connection_w_founder_if_any.dta, replace

log close

