# Lyteflow
Pipeline system for sequential data transformations
## Description
Lyteflow is to ensure consistent, simple, and sequential data transformations. A System
 of pipe elements can be created, and placed in a pipe system. The flow starts from 
 Inlets, flows through the individual pipe elements, and ends in an Outlet. The flow 
 from individual pipe elements are universally compatible. The pipe elements will 
 attempt to pass a Numpy array or a pandas DataFrame to each other (depending on the 
 context).
## Installation
```bash
git clone https://github.com/MammothStack/Lyteflow.git Lyteflow
```
## Usages
```python
from Lyteflow import PipeSystem, Inlet, Outlet, Normalizer, Rotator

images = get_data()
in_1 = Inlet(convert=False)
x = Normalizer(scale_from=(0,255), scale_to=(0,1))(in_1)
x = Rotator([-10, -5, 5, 10], remove_padding=True, keep_original=True)(x)
out_1 = Outlet()
ps = PipeSystem([in_1], [out_1])

processed_images = ps.flow(images)
```
## Support
## Contributing
## Authors
Patrick Bogner
## License
[MIT](https://choosealicense.com/licenses/mit/)
## Status
Development ongoing