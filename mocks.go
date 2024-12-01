// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/lovoo/goka (interfaces: TopicManager,Producer,Broker)
//
// Generated by this command:
//
//	mockgen -self_package github.com/lovoo/goka -package goka -destination mocks.go github.com/lovoo/goka TopicManager,Producer,Broker
//

// Package goka is a generated GoMock package.
package goka

import (
	reflect "reflect"

	sarama "github.com/IBM/sarama"
	gomock "go.uber.org/mock/gomock"
)

// MockTopicManager is a mock of TopicManager interface.
type MockTopicManager struct {
	ctrl     *gomock.Controller
	recorder *MockTopicManagerMockRecorder
	isgomock struct{}
}

// MockTopicManagerMockRecorder is the mock recorder for MockTopicManager.
type MockTopicManagerMockRecorder struct {
	mock *MockTopicManager
}

// NewMockTopicManager creates a new mock instance.
func NewMockTopicManager(ctrl *gomock.Controller) *MockTopicManager {
	mock := &MockTopicManager{ctrl: ctrl}
	mock.recorder = &MockTopicManagerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockTopicManager) EXPECT() *MockTopicManagerMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockTopicManager) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockTopicManagerMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockTopicManager)(nil).Close))
}

// EnsureStreamExists mocks base method.
func (m *MockTopicManager) EnsureStreamExists(topic string, npar int) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "EnsureStreamExists", topic, npar)
	ret0, _ := ret[0].(error)
	return ret0
}

// EnsureStreamExists indicates an expected call of EnsureStreamExists.
func (mr *MockTopicManagerMockRecorder) EnsureStreamExists(topic, npar any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "EnsureStreamExists", reflect.TypeOf((*MockTopicManager)(nil).EnsureStreamExists), topic, npar)
}

// EnsureTableExists mocks base method.
func (m *MockTopicManager) EnsureTableExists(topic string, npar int) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "EnsureTableExists", topic, npar)
	ret0, _ := ret[0].(error)
	return ret0
}

// EnsureTableExists indicates an expected call of EnsureTableExists.
func (mr *MockTopicManagerMockRecorder) EnsureTableExists(topic, npar any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "EnsureTableExists", reflect.TypeOf((*MockTopicManager)(nil).EnsureTableExists), topic, npar)
}

// EnsureTopicExists mocks base method.
func (m *MockTopicManager) EnsureTopicExists(topic string, npar, rfactor int, config map[string]string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "EnsureTopicExists", topic, npar, rfactor, config)
	ret0, _ := ret[0].(error)
	return ret0
}

// EnsureTopicExists indicates an expected call of EnsureTopicExists.
func (mr *MockTopicManagerMockRecorder) EnsureTopicExists(topic, npar, rfactor, config any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "EnsureTopicExists", reflect.TypeOf((*MockTopicManager)(nil).EnsureTopicExists), topic, npar, rfactor, config)
}

// GetOffset mocks base method.
func (m *MockTopicManager) GetOffset(topic string, partitionID int32, time int64) (int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetOffset", topic, partitionID, time)
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetOffset indicates an expected call of GetOffset.
func (mr *MockTopicManagerMockRecorder) GetOffset(topic, partitionID, time any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetOffset", reflect.TypeOf((*MockTopicManager)(nil).GetOffset), topic, partitionID, time)
}

// Partitions mocks base method.
func (m *MockTopicManager) Partitions(topic string) ([]int32, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Partitions", topic)
	ret0, _ := ret[0].([]int32)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Partitions indicates an expected call of Partitions.
func (mr *MockTopicManagerMockRecorder) Partitions(topic any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Partitions", reflect.TypeOf((*MockTopicManager)(nil).Partitions), topic)
}

// MockProducer is a mock of Producer interface.
type MockProducer struct {
	ctrl     *gomock.Controller
	recorder *MockProducerMockRecorder
	isgomock struct{}
}

// MockProducerMockRecorder is the mock recorder for MockProducer.
type MockProducerMockRecorder struct {
	mock *MockProducer
}

// NewMockProducer creates a new mock instance.
func NewMockProducer(ctrl *gomock.Controller) *MockProducer {
	mock := &MockProducer{ctrl: ctrl}
	mock.recorder = &MockProducerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockProducer) EXPECT() *MockProducerMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockProducer) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockProducerMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockProducer)(nil).Close))
}

// Emit mocks base method.
func (m *MockProducer) Emit(topic, key string, value []byte) *Promise {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Emit", topic, key, value)
	ret0, _ := ret[0].(*Promise)
	return ret0
}

// Emit indicates an expected call of Emit.
func (mr *MockProducerMockRecorder) Emit(topic, key, value any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Emit", reflect.TypeOf((*MockProducer)(nil).Emit), topic, key, value)
}

// EmitWithHeaders mocks base method.
func (m *MockProducer) EmitWithHeaders(topic, key string, value []byte, headers Headers) *Promise {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "EmitWithHeaders", topic, key, value, headers)
	ret0, _ := ret[0].(*Promise)
	return ret0
}

// EmitWithHeaders indicates an expected call of EmitWithHeaders.
func (mr *MockProducerMockRecorder) EmitWithHeaders(topic, key, value, headers any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "EmitWithHeaders", reflect.TypeOf((*MockProducer)(nil).EmitWithHeaders), topic, key, value, headers)
}

// MockBroker is a mock of Broker interface.
type MockBroker struct {
	ctrl     *gomock.Controller
	recorder *MockBrokerMockRecorder
	isgomock struct{}
}

// MockBrokerMockRecorder is the mock recorder for MockBroker.
type MockBrokerMockRecorder struct {
	mock *MockBroker
}

// NewMockBroker creates a new mock instance.
func NewMockBroker(ctrl *gomock.Controller) *MockBroker {
	mock := &MockBroker{ctrl: ctrl}
	mock.recorder = &MockBrokerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockBroker) EXPECT() *MockBrokerMockRecorder {
	return m.recorder
}

// Addr mocks base method.
func (m *MockBroker) Addr() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Addr")
	ret0, _ := ret[0].(string)
	return ret0
}

// Addr indicates an expected call of Addr.
func (mr *MockBrokerMockRecorder) Addr() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Addr", reflect.TypeOf((*MockBroker)(nil).Addr))
}

// Connected mocks base method.
func (m *MockBroker) Connected() (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Connected")
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Connected indicates an expected call of Connected.
func (mr *MockBrokerMockRecorder) Connected() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Connected", reflect.TypeOf((*MockBroker)(nil).Connected))
}

// CreateTopics mocks base method.
func (m *MockBroker) CreateTopics(request *sarama.CreateTopicsRequest) (*sarama.CreateTopicsResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateTopics", request)
	ret0, _ := ret[0].(*sarama.CreateTopicsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CreateTopics indicates an expected call of CreateTopics.
func (mr *MockBrokerMockRecorder) CreateTopics(request any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateTopics", reflect.TypeOf((*MockBroker)(nil).CreateTopics), request)
}

// Open mocks base method.
func (m *MockBroker) Open(conf *sarama.Config) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Open", conf)
	ret0, _ := ret[0].(error)
	return ret0
}

// Open indicates an expected call of Open.
func (mr *MockBrokerMockRecorder) Open(conf any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Open", reflect.TypeOf((*MockBroker)(nil).Open), conf)
}
