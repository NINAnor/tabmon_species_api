import librosa

def extract_clip(audio_file_path, start_time, sr=48000):
    audio_data, _ = librosa.load(audio_file_path, sr=sr, mono=True)
    start_sample = int(start_time * sr)
    end_sample = int((start_time + 3) * sr)
    return audio_data[start_sample:end_sample]


