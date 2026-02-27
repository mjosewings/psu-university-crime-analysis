-- ============================================================================
-- PSU Campus Crime Data Cleanup: Standardize Campus Codes
-- ============================================================================
-- Problem: The scraper created duplicate campus entries with inconsistent codes.
-- This script remaps all incidents to the canonical campus IDs and removes
-- the duplicate rows.
--
-- Correct campus code mapping:
--   UP  = University Park     AB  = Abington          AL  = Altoona
--   BK  = Beaver              BE  = Erie (Behrend)    BR  = Berks
--   BW  = Brandywine          DL  = Dickinson Law     DB  = DuBois
--   FA  = Fayette             GA  = Greater Allegheny  GV  = Great Valley
--   HB  = Harrisburg          HZ  = Hazleton          HS  = Hershey
--   LV  = Lehigh Valley       MA  = Mont Alto         NK  = New Kensington
--   SK  = Schuylkill          SH  = Shenango          WB  = Wilkes-Barre
--   WS  = Worthington Scranton  YK = York
--
-- Duplicate campus entries found:
--   campus_id 47  (HN)   -> should be campus_id 15 (HS)  Hershey
--   campus_id 48  (ER)   -> should be campus_id 5  (BE)  Erie (Behrend)
--   campus_id 49  (BKT)  -> should be campus_id 4  (BK)  Beaver
--   campus_id 50  (SL)   -> should be campus_id 19 (SK)  Schuylkill
--   campus_id 51  (DS)   -> should be campus_id 9  (DB)  DuBois
--   campus_id 52  (FE)   -> should be campus_id 10 (FA)  Fayette
--   campus_id 54  (PSHI) -> should be campus_id 1  (UP)  University Park
--   campus_id 146 (ABT)  -> should be campus_id 2  (AB)  Abington
-- ============================================================================

-- Step 1: Remap incidents from duplicate campus IDs to the correct canonical IDs
UPDATE incidents SET campus_id = 15 WHERE campus_id = 47;   -- HN  -> HS (Hershey)
UPDATE incidents SET campus_id = 5  WHERE campus_id = 48;   -- ER  -> BE (Erie/Behrend)
UPDATE incidents SET campus_id = 4  WHERE campus_id = 49;   -- BKT -> BK (Beaver)
UPDATE incidents SET campus_id = 19 WHERE campus_id = 50;   -- SL  -> SK (Schuylkill)
UPDATE incidents SET campus_id = 9  WHERE campus_id = 51;   -- DS  -> DB (DuBois)
UPDATE incidents SET campus_id = 10 WHERE campus_id = 52;   -- FE  -> FA (Fayette)
UPDATE incidents SET campus_id = 1  WHERE campus_id = 54;   -- PSHI -> UP (University Park)
UPDATE incidents SET campus_id = 2  WHERE campus_id = 146;  -- ABT -> AB (Abington)

-- Step 2: Remove the duplicate campus entries
DELETE FROM campuses WHERE campus_id IN (47, 48, 49, 50, 51, 52, 54, 146);

-- Step 3: Remove junk row (incident with no real data, incident_number = ':')
DELETE FROM incident_offenses WHERE incident_id = 1;
DELETE FROM incidents WHERE id = 1;

-- Step 4: Verify the cleanup
-- Should return exactly 23 campuses with correct codes
SELECT campus_id, campus_code, campus_name FROM campuses ORDER BY campus_code;

-- Should return 0 (no incidents pointing to non-existent campuses)
SELECT COUNT(*) AS orphaned_incidents
FROM incidents i
LEFT JOIN campuses c ON i.campus_id = c.campus_id
WHERE c.campus_id IS NULL;

-- Incident counts per campus after cleanup
SELECT c.campus_code, c.campus_name, COUNT(i.id) AS incident_count
FROM campuses c
LEFT JOIN incidents i ON c.campus_id = i.campus_id
GROUP BY c.campus_id, c.campus_code, c.campus_name
ORDER BY incident_count DESC;
