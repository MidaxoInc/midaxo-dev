SELECT distinct last_day(a.eventdate,'week') AS ddate,
                count(distinct a.user_id) over (partition BY ddate, usage_event) AS active_user,
                count(distinct a.company_id) over (partition BY ddate, usage_event) AS active_org
FROM midaxo.dev.event_usage a
WHERE a.usage_event = 'login'
  AND a.company_id not in ('e8af76d7-9034-4ead-93f2-09d7138a69a8',
                           'c852dc55-277b-4570-a63e-ob93be8d925f',
                           '53137d73-5ebo-48a9-b778-0de844a83fd9',
                           'c3a54b4b-fob6-40e6-b09b-2e58e9b15348',
                           '4514619c-8723-4df7-a7c4-3034dc2cd3a5',
                           'd8679c78-f5f1-46b6-b09f-4b6c63215450',
                           'c794c216-adoo-4e58-a133-4e662bf6c7bc',
                           '42af86ab-6aob-4568-8a51-50e4d5cdddd3',
                           'fbf47091-2fdo-45a1-93fc-55a8a074e818',
                           '959a4126-85f2-4913-b1c3-5a940095d84d',
                           '995ceb85-be4f-45bf-a8cc-5ff8e37559b9',
                           '66907e6a-96ee-4628-adfa-661a8b7c3b90',
                           'caf79bbe-2a6f-4430-abfo-67a600836689',
                           '2a42f93e-a870-4324-a9ad-754dcec6c4f7',
                           '7a5afee5-7cb6-4afb-8f40-7f750b8c6c10',
                           '1d2c3d91-3bf9-4d7c-86e1-80be325a94c7',
                           '24d517ef-6e22-479b-9ce5-86688d328c5a',
                           '721e39f5-4801-43a1-9c5b-8803327e3180',
                           '88e8ecb9-d790-4a4e-aef5-8a1ff7511286',
                           '1917346f-adbf-48a8-8ca5-8ff61',
                           'c70d53d5-e4e6-43f9-a480-95559a63a891',
                           '10311abo-b77b-4201-a5d3-96020c42a201',
                           'ddc06154-6bf3-4dda-b593-96168609a118',
                           '2a69462e-oabb-4d34-b8a9-96728e2b1a32',
                           '6a344496-8feb-43c6-8736-9baca633abaf',
                           'ec95056e-023c-490a-9871-9c3ea322a4ae',
                           '3405209b-22f4-4a86-ab7b-9ed1827d63b7',
                           'a3ccd2f9-d3c4-42fa-a14a-a31196dcc255',
                           'd29a6b87-b63a-426d-b4e3-a692e2c49e41',
                           '490d9c7c-cof7-49bc-a3da-b39d48c03299',
                           '6eb9cb58-ff78-4fbd-a5d1-bbdc29382435',
                           '3a824461-0fdf-478d-9f62-bc65654c5716',
                           '2da9d99e-5ac2-410a-a05f-c30269eb0055',
                           'b7bc30b7-oca7-4258-8b52-cda0793529db',
                           '55aadoe6-e4d6-429f-9fa5-cdf48ccd11',
                           'd1392dfa-b1c3-4614-8093-d4cd8d3f9a45',
                           '534cde35-862b-4afb-8eec-db2560c52391',
                           'accf8469-635b-4ec7-b2b9-f44774a9289b',
                           'd15ef952-e273-4a90-879d-fboob2675989',
                           'bf9e9f50-1464-4bof-a060-fd25dbc34360',
                           '3cb51697-3bfa-425c-99f2-dbbdfd6b32c8',
                           '74470aab-27e4-4c9b-ma7-dc05e2c04c76',
                           '80977232-8509-4400-81 a1-dc2306bd6e2b',
                           '35ddc181-5f59-4ec7-957a-de0137e53aoo',
                           '93202d59-159b-423a-8doc-e04db9f24002',
                           'b51oc1 cd-c2dc-41a7-b45d-e18b14842f73',
                           'b42a88b9-9718-40eo-b332-e708af99498f',
                           '97090ba2-2ac7-49m-md8-e93da20ba6b6',
                           'd3d57ecd-c844-4c3e-8d09-ecac8f175bd9',
                           '890572c5-17c2-487d-b1fd-ed9346c1ob2e',
                           'b1mbcaa-9be4-4c57-b5e3-ee6bobe27702',
                           'e9aoob72-f1 d6-4c8b-9227-ef3a623874a7',
                           'e4d9852b-cbeb-4d26-b66e-oa571',
                           '1c6e9ceb-9df5-46b1-bf90-14003dcceae3',
                           'c1df1b8f-3859-43ff-b49d-2460804188ab',
                           '45ac8e52-694a-43fc-9fd5-311f225a8493',
                           '77eoe313-b726-48d5-bobo-5f5c3cb48c8c',
                           'db6b573b-9526-49ef-8f38-73e9ff7672ff',
                           '2bd4c58b-a43e-467e-98f8-75fb97143b36',
                           'a3589e9b-9a8d-4870-9803-7db78c669976',
                           'fb178934-ab9c-412a-8f42-93567e729a5f',
                           '038coe23-db02-4d56-8c1d-9427aofafa8c',
                           'cd8c5024-07a8-47do-b8e3-9b2e7fca9f39',
                           '7f4e1701-7f99-40f9-b27e-9bd89ff86c3a',
                           'a8195fa6-062d-4a8b-95f7-9c7ac3f57bb3',
                           'dob77bee-5d4b-4a03-b5ae-a10448583265',
                           '8d98ae8a-ad98-40e7-b406-a6e4d285519b',
                           '7eb3f3e1-d3do-4e93-a65d-ac4703c50f59',
                           '490d9c7c-cof7-49bc-a3da-b39d48c03299',
                           'e9c42016-33be-4a3b-a158-c3edcd8ab781',
                           '28edfaco-5cea-4fa9-a532-cd54f13c1bd7',
                           '55ba26ed-86c4-4638-a1cc-d98edbdcd25b',
                           'f80e7fc4-0363-4221-97db-e5da6ec73b86')
  AND last_day(a.eventdate,'week') < current_date
ORDER BY ddate ASC