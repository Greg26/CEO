/*[Data Product 3: This is an update of DP1 using DP2. The same rows as in DP1, with an additional
 column: type_of_ceo={founder,insider,outsider}]
*/

clear all
set more off

cap log using ${LOG}3_Data_Product, replace

use ${OUTPUT}Data_Product_1.dta, clear

merge m:1 frame_id person_id using ${OUTPUT}Data_product_2

g type_of_ceo = "founder" if _merge == 3

drop if _merge == 2
drop _merge

preserve

*Appended_mandb_owner prepares in 1_...do
use ${OUTPUT}Appended_mandb_owner.dta, clear

bys frame_id person_id: egen first_appears_in_frame = min(start_date)

collapse first_appears_in_frame, by(frame_id person_id)

tempfile first_appears
save `first_appears'

restore

merge m:1 frame_id person_id using `first_appears'
format %td first_appears_in_frame

drop if _merge == 2

replace type_of_ceo = "insider" if type_of_ceo =="" & first_appears_in_frame < start_date
replace type_of_ceo = "outsider" if type_of_ceo == ""



save ${OUTPUT}Data_product_3, replace

log close
