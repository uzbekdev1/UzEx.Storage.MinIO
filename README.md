# UzEx Storage MinIO client

```
var client = new MinStoreClient("min://{host}/{root}={psw}/{buket}");
var prefix = 12;

// upload
await client.Upload("images", $"{prefix}-demo.bmp", File.OpenRead("demo.bmp"), "image/bmp");

// list
var list = client.Search("images", $"{prefix}");
foreach (var item in list)
{
Console.WriteLine(item);
}

// download
var stream = await client.Download("images", $"{prefix}-demo.bmp");
var data = new byte[stream.Length];
await stream.WriteAsync(data, 0, data.Length);
await File.WriteAllBytesAsync($"{Guid.NewGuid()}.bmp", data);

// delete
await client.Delete("images", $"{prefix}-demo.bmp");
```
