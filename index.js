const
	S3rver = require("s3rver"),
	{S3Client, HeadObjectCommand, GetObjectCommand} = require("@aws-sdk/client-s3"),
	parquet = require("parquetjs-lite"),

	util = require("util"),

	LOCAL_LISTEN_PORT = 4569,
	TEST_BUCKET = "test",
	
	s3rver = new S3rver({
		port: LOCAL_LISTEN_PORT,
		address: '127.0.0.1',
		silent: true,
		directory: __dirname,
		configureBuckets: [
			{
				name: TEST_BUCKET
			},
		],
	}),
	
	s3 = new S3Client({
		forcePathStyle: true,
		credentials: {
			accessKeyId: "S3RVER",
			secretAccessKey: "S3RVER"
		},
		endpoint: 'http://localhost:' + LOCAL_LISTEN_PORT,
	});

async function test(fromS3) {
	let 
		reader;
	
	if (fromS3) {
		await (util.promisify(s3rver.run.bind(s3rver))());
		
		reader = await parquet.ParquetReader.openS3(
			{S3Client: s3, HeadObjectCommand, GetObjectCommand},
			{
				Bucket: TEST_BUCKET,
				Key: "completed-files"
			}
		);
	} else {
		reader = await parquet.ParquetReader.openFile("test/completed-files._S3rver_object");
	}
		
	let
		cursor = reader.getCursor(),	
		record;
	
	while (record = await cursor.next()) {
		// Just iterate the rows, do nothing with them
	}
	
	await reader.close();
	
	if (fromS3) {
		await (util.promisify(s3rver.close.bind(s3rver))());
	}
}

test(false)
	.then(() => console.log("Read successfully from file"), err => console.error("File error: " + err.stack))
	.then(() => test(true))
	.then(() => console.log("Read successfully from S3"), err => console.error("S3 error: " + err.stack));
