export $airflow_server = locahost:8080
curl --location --request POST 'http://localhost:8080/api/v1/dags/example_databricks_repos_operator_v2/dagRuns' \
--header 'Authorization: Basic YWRtaW46dGlkZQ==' \
--header 'Content-Type: application/json' \
--header 'Cookie: session=.eJwNy0EKgCAQBdC7zLqNaVpeRmz6Q5BZqK2iu-f2wXspSEHdyUtMFQOFG-WMGbmRb-XpwrVIaNeBTJ7UqLHaUYl2s1p4AqxRZnFu1Rw3YbazgbZCA6WLY0I_PX4_EAog2A.YwcyNQ.DUrzhmoY16JL7mP6FVRYBFfHndM' \
--data-raw '{
  "conf": {}
}'