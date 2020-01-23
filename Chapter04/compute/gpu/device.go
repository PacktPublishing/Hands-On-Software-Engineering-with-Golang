package gpu

// Device describes a GPU computing node.
type Device struct{}

// NewDevice returns a new GPU-backed device instance.
func NewDevice() *Device {
	return new(Device)
}

// Square implements the compute.DeviceDevice interface.
func (*Device) Square(in []float32) []float32 {
	panic("not implemented")
}

// Sum implements the compute.DeviceDevice interface.
func (*Device) Sum(in []float32) float32 {
	panic("not implemented")
}
