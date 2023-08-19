from censaurus.tiger import AreaCollection

def break_count_into_chunks(count):
    num_chunks = 50
    chunk_size = count // num_chunks
    remainder = count % num_chunks

    chunks = []
    start = 1
    for i in range(num_chunks + 1):
        chunk_end = start + chunk_size + (1 if i < remainder else 0)
        chunks.append(start)
        start = chunk_end

    return chunks

a = AreaCollection()
for l_name, l in a.available_layers.items():
    resp = l.tiger_client.get_sync(f'{l.id}/query', params={
        'where': '1=1',
        'returnCountOnly': 'true'
    })
    count = resp.json()['count']
    chunks = break_count_into_chunks(count=count)

    left, right = 0, len(chunks) - 1
    highest = None
    while left <= right:
        mid = (left + right) // 2
        result_count = chunks[mid]
        try:
            resp = l.tiger_client.get_sync(f'{l.id}/query', params={
                'where': '1=1',
                'outFields': 'GEOID',
                'returnGeometry': 'true',
                'geometryPrecision': '6',
                'outSR': '4236',
                'resultRecordCount': f'{result_count}'
            })
            highest = result_count
            left = mid + 1
        except:
            right = mid - 1
    print(f'{l_name}: {result_count} of {count}')