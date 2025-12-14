import torch

if torch.cuda.is_available():
    print(f"✅ Success! Found GPU: {torch.cuda.get_device_name(0)}")
    print(f"   VRAM: {torch.cuda.get_device_properties(0).total_memory / 1024**3:.2f} GB")
else:
    print("❌ Error: PyTorch is using the CPU. The manual install didn't stick.")