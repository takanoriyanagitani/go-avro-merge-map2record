#!/bin/sh

genjson(){

	jname=./sample.d/sample.jsonl

	test -f "${jname}" && rm "${jname}"

	data1=$(
		jq -c -n '{
			attr: {
				user: { id: 42 },
				http: {
					method: "GET",
					url: "https://localhost:8080/products/3776",
					status: 404,
					version: "HTTP/2",
					query: [
						"tag=audio",
						"tag=wireless",
						"tag=ble",
						"tag=snyppl"
					],
				},
			},
			resource: {
				service: {
					name: "web-svc",
					ver: "0.1.0",
				},
			},
		}'
	)

	data2=$(
		jq -c -n '{
			attr: {
				user: { id: 43 },
				http: {
					method: "GET",
					url: "https://localhost:8080/products/3777",
					status: 403,
					version: "HTTP/1.1",
					query: [
						"tag=audio",
						"tag=wireless",
						"tag=ble",
						"tag=snyppl"
					],
				},
			},
			resource: {
				service: {
					name: "web-svc",
					ver: "0.1.0",
				},
			},
		}'
	)

	jq \
		-n \
		-c \
		--arg dat "${data1}" \
		'{
			date: "2024-12-27",
			level: "WARN",
			body: "invalid url",
			data: $dat,
		}' |
	cat >> "${jname}"

	jq \
		-n \
		-c \
		--arg dat "${data2}" \
		'{
			date: "2024-12-27",
			level: "WARN",
			body: "invalid url",
			data: $dat,
		}' |
	cat >> "${jname}"

}

jsons2avro() {
	export ENV_BLOB_KEY=data
	cat sample.d/sample.jsonl |
			ENV_SCHEMA_FILENAME=sample.d/input.avsc json2avrows |
			ENV_SCHEMA_FILENAME=sample.d/mid.avsc avro2blob2json2flatmap |
		cat >./sample.d/input.avro
}

#genjson
#jsons2avro

export ENV_MAP_COLNAME=data
export ENV_SCHEMA_FILENAME=sample.d/output.avsc

cat sample.d/input.avro |
	./avro2mapcol2record |
	cat > ./sample.d/output.avro

cat ./sample.d/output.avro |
	avro2jsons |
	dasel --read=json --write=yaml |
	bat --language=yaml
