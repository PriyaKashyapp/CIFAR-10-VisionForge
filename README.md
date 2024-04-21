# CIFAR-10-VisionForge

CIFAR-10 Vision Forge is a deep learning project that optimizes a neural network to classify images from the CIFAR-10 dataset, enhancing model architecture and tuning hyperparameters to achieve a peak testing accuracy of 86.14%

Technologies Used

    Python: Primary programming language.
    PyTorch: For building and training neural network models.
    Matplotlib: For plotting training and validation metrics.

Dataset

The CIFAR-10 dataset consists of 60,000 32x32 color images across 10 different classes, with 50,000 images for training and 10,000 for testing. We applied random horizontal flips for data augmentation and used PyTorch's DataLoader for efficient batch processing.

Model Architecture

Basic Architecture

    Intermediate Blocks: Consists of multiple convolutional layers with coefficients computed dynamically, contributing to a single output image.
    
    Output Block: Processes the final image from the intermediate blocks to produce a logits vector.

Enhanced Architecture

    Layer Details: Includes batch normalization, ReLU activation, and max-pooling for effective feature learning and dimensionality reduction.
    
    Dropout: Integrated with a rate of 0.5 to prevent overfitting.

Hyperparameters and Optimization

    Learning Rate: Initially tested various rates, with the optimal rate found at 0.003.
    
    Optimizer: Adam, providing robust updates to the weights.
    
    Epochs: Set to 50 for thorough training.
    
    Loss Function: Cross-Entropy Loss, ideal for multi-class classification.

The optimized neural network model successfully achieved a remarkable testing accuracy of 86.14%, demonstrating the effectiveness of the enhanced architectural modifications and hyperparameter tuning implemented throughout the project. This result underscores the model's capability to accurately classify complex image data across diverse categories in the CIFAR-10 dataset.


