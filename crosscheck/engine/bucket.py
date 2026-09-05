def get_bucket_range(min_id: int, bucket_size: int, bucket_idx: int) -> tuple[int, int]:
    start = min_id + bucket_idx * bucket_size
    end = start + bucket_size - 1
    return start, end
