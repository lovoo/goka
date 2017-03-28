package kafka

import (
	"fmt"
	"testing"

	"stash.lvint.de/lab/goka/kafka/mock"

	kazoo "github.com/db7/kazoo-go"
	"github.com/facebookgo/ensure"
	"github.com/golang/mock/gomock"
)

func TestTopicManager_updateChroot(t *testing.T) {
	chroot := "/test"
	host := "somehost.com"
	chroot2 := "/test2"
	host2 := "somehost2.com"

	// chroot in one server
	servers := []string{host + chroot}
	s, c, err := updateChroot(servers)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, c, chroot)
	ensure.DeepEqual(t, s[0], host)

	// chroot in one out of multiple servers
	servers = []string{host + chroot, host2}
	s, c, err = updateChroot(servers)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, c, chroot)
	ensure.DeepEqual(t, s[0], host)
	ensure.DeepEqual(t, s[1], host2)

	// chroot in multiple servers
	servers = []string{host + chroot, host2 + chroot}
	s, c, err = updateChroot(servers)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, c, chroot)
	ensure.DeepEqual(t, s[0], host)
	ensure.DeepEqual(t, s[1], host2)

	// chroot in multiple servers but different
	servers = []string{host + chroot, host2 + chroot2}
	_, _, err = updateChroot(servers)
	ensure.NotNil(t, err)

	// check chroot in servers but parse fails
	servers = []string{host + chroot, host2 + "/hi/whatever"}
	_, _, err = updateChroot(servers)
	ensure.NotNil(t, err)

}

func TestCheckPartitions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	client := mock.NewMockClient(ctrl)

	topic := "topic"
	npar := 3

	client.EXPECT().Partitions(topic).Return([]int32{0, 1, 2}, nil)
	err := checkPartitions(client, topic, npar)
	fmt.Println(err)
	ensure.Nil(t, err)

	client.EXPECT().Partitions(topic).Return([]int32{0, 1}, nil)
	err = checkPartitions(client, topic, npar)
	ensure.NotNil(t, err)
	ensure.StringContains(t, err.Error(), "partitions instead")

	client.EXPECT().Partitions(topic).Return([]int32{0, 1, 2}, fmt.Errorf("some error in the wire"))
	err = checkPartitions(client, topic, npar)
	ensure.NotNil(t, err)
	ensure.StringContains(t, err.Error(), "Error fetching")
}

func TestTopicManager_hasTopic(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	kz := mock.NewMockkzoo(ctrl)

	topic := "test"

	kz.EXPECT().Topics().Return(kazoo.TopicList{&kazoo.Topic{Name: topic}}, nil)
	ok, err := hasTopic(kz, topic)
	ensure.Nil(t, err)
	ensure.True(t, ok)

	kz.EXPECT().Topics().Return(kazoo.TopicList{&kazoo.Topic{Name: "other"}}, nil)
	ok, err = hasTopic(kz, topic)
	ensure.Nil(t, err)
	ensure.False(t, ok)

	kz.EXPECT().Topics().Return(kazoo.TopicList{&kazoo.Topic{Name: topic}}, fmt.Errorf("some error"))
	_, err = hasTopic(kz, topic)
	ensure.NotNil(t, err)
}

func TestCheckTopic(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	kz := mock.NewMockkzoo(ctrl)

	topic := "test"

	kz.EXPECT().Topics().Return(kazoo.TopicList{&kazoo.Topic{Name: topic}}, nil)
	ok, err := hasTopic(kz, topic)
	ensure.Nil(t, err)
	ensure.True(t, ok)

	kz.EXPECT().Topics().Return(kazoo.TopicList{&kazoo.Topic{Name: "other"}}, nil)
	ok, err = hasTopic(kz, topic)
	ensure.Nil(t, err)
	ensure.False(t, ok)

	kz.EXPECT().Topics().Return(kazoo.TopicList{&kazoo.Topic{Name: topic}}, fmt.Errorf("some error"))
	_, err = hasTopic(kz, topic)
	ensure.NotNil(t, err)
}

/* cannot fix this yet
type topicMock struct {
	*kazoo.Topic
}

func (t *topicMock) Partitions() (kazoo.PartitionList, error) {
	return kazoo.PartitionList{}, nil
}
func TestTopicManager_checkPartitions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	kz := mock_kazoo.NewMockKazoo(ctrl)
	topic := "test"
	npar := 3
	ac := &topicManager{
		servers: []string{"somehost"},
		config:  NewTopicManagerConfig(),
	}

	gomock.InOrder(
		kz.EXPECT().Topic(topic).Return(&topicMock{&kazoo.Topic{Name: topic}}),
	)
	err := ac.checkPartitions(kz, topic, npar)
	ensure.Nil(t, err)
}
*/
