# CIFAR-10-VisionForge

CIFAR-10 Vision Forge is a deep learning project that optimizes a neural network to classify images from the CIFAR-10 dataset, enhancing model architecture and tuning hyperparameters to achieve a peak testing accuracy of 86.14%

Technologies Used

    Python: Primary programming language.
    PyTorch: For building and training neural network models.
    Matplotlib: For plotting training and validation metrics.

Dataset

The CIFAR-10 dataset, utilized in this project, comprises 60,000 32x32 color images spanning 10 different classes (airplane, automobile, bird, cat, deer, dog, frog, horse, ship, truck), with each class containing 6,000 images. This dataset is split into 50,000 training images and 10,000 testing images. It was sourced from the official CIFAR-10 dataset page hosted by the Canadian Institute for Advanced Research (CIFAR) at University of Toronto. We employed data augmentation techniques such as random horizontal flips to enhance the model's generalization capabilities during training.

Link : https://www.cs.toronto.edu/~kriz/cifar.html

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


