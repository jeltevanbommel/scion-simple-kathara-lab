package main

/*
#cgo LDFLAGS: -lnetfilter_queue
#include <stdlib.h>
#include <errno.h>
#include <arpa/inet.h>
#include <linux/netfilter.h>
#include <libnetfilter_queue/libnetfilter_queue.h>

extern uint32_t goVerdict(uint32_t id, unsigned char* data, int len);

static int cb(struct nfq_q_handle *qh, struct nfgenmsg *nfmsg,
              struct nfq_data *nfa, void *data) {
    unsigned char *payload = NULL;
    int len = nfq_get_payload(nfa, &payload);
    uint32_t id = 0;
    struct nfqnl_msg_packet_hdr *ph = nfq_get_msg_packet_hdr(nfa);
    if (ph) {
        id = ntohl(ph->packet_id);
    }
    uint32_t verdict = goVerdict(id, payload, len);
    return nfq_set_verdict(qh, id, verdict, 0, NULL);
}

static int runQueue(int queueNum) {
    struct nfq_handle *h = nfq_open();
    if (!h) return -1;
    nfq_unbind_pf(h, AF_INET);
    if (nfq_bind_pf(h, AF_INET) < 0) {
        nfq_close(h);
        return -2;
    }

    struct nfq_q_handle *qh = nfq_create_queue(h, queueNum, &cb, NULL);
    if (!qh) {
        nfq_close(h);
        return -3;
    }

    if (nfq_set_mode(qh, NFQNL_COPY_PACKET, 0xffff) < 0) {
        nfq_destroy_queue(qh);
        nfq_close(h);
        return -4;
    }

    int fd = nfq_fd(h);
    char buf[4096] __attribute__((aligned));
    while (1) {
        int rv = recv(fd, buf, sizeof(buf), 0);
        if (rv < 0) {
            if (errno == EINTR) continue;
            break;
        }
        nfq_handle_packet(h, buf, rv);
    }

    nfq_destroy_queue(qh);
    nfq_close(h);
    return 0;
}
*/
import "C"

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"strconv"
	"unsafe"
)

const maxSeq = 5

//export goVerdict
func goVerdict(id C.uint32_t, data *C.uchar, length C.int) C.uint32_t {
	if data == nil || length <= 0 {
		return C.uint32_t(C.NF_ACCEPT)
	}

	b := C.GoBytes(unsafe.Pointer(data), length)
	if len(b) < 20 {
		return C.uint32_t(C.NF_ACCEPT)
	}

	ihl := int(b[0]&0x0f) * 4
	if ihl < 20 || len(b) < ihl+8 {
		return C.uint32_t(C.NF_ACCEPT)
	}

	if b[9] != 1 {
		return C.uint32_t(C.NF_ACCEPT)
	}

	icmpType := b[ihl]
	if icmpType != 8 {
		return C.uint32_t(C.NF_ACCEPT)
	}

	seq := binary.BigEndian.Uint16(b[ihl+6 : ihl+8])
	src := net.IP(b[12:16]).String()
	dst := net.IP(b[16:20]).String()

	verdict := C.uint32_t(C.NF_ACCEPT)
	if seq >= maxSeq {
		verdict = C.uint32_t(C.NF_DROP)
	}

	fmt.Printf("icmp type=%d seq=%d %s -> %s verdict=%s\n",
		icmpType, seq, src, dst, verdictString(verdict))
	return verdict
}

func verdictString(v C.uint32_t) string {
	if v == C.NF_DROP {
		return "DROP"
	}
	return "ACCEPT"
}

func main() {
	queueNum := 0
	if len(os.Args) > 1 {
		if parsed, err := strconv.Atoi(os.Args[1]); err == nil {
			queueNum = parsed
		}
	}
	if queueNum == 0 {
		queueNum = 10
	}
	fmt.Printf("nfqueue_reader: queue %d\n", queueNum)
	C.runQueue(C.int(queueNum))
}
