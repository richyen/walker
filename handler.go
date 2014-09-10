package walker

type Handler interface {
	HandleResponse(res *FetchResults)
}

type SimpleWriterHandler struct{}

func (h *SimpleWriterHandler) HandleResponse(res *FetchResults) {
	//TODO: implement this
}
