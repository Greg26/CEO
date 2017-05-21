/*/[Data Product 4: An update of DP3. Additional columns: years_of_experience (time spent as CEO before current spell),
 number_of_ceo_jobs (number of distinct frameids managed before current spell), 
 previous_business_with_founders={0,1}. Calculate these for all CEOs, not just outsiders.]
*/
 
clear all 
set more off

cap log using ${LOG}4_Data_Product, replace

use ${OUTPUT}Data_product_3.dta, clear

g length_of_spell = end_date - start_date 

bys person_id : egen years_of_experience = sum(length_of_spell)
bys person_id : egen missing_boundary = max( cond( mi(length_of_spell), 1, 0 ) ) 

replace years_of_experience = . if missing_boundary == 1
replace years_of_experience = round(years_of_experience/365, .01)

sort person_id frame_id
egen new_person_frame = tag(person_id frame_id)
bys person_id: egen n_th_frame = seq() if new_person_frame == 1

g number_of_ceo_jobs = n_th_frame - 1
bys person_id: carryforward number_of_ceo_jobs, replace

g byte previous_business_with_founders
