package main

/*
#cgo LDFLAGS: -lnetfilter_queue
#include <stdlib.h>
#include <errno.h>
#include <arpa/inet.h>
#include <linux/netfilter.h>
#include <libnetfilter_queue/libnetfilter_queue.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>

static int g_debug = 0;

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
    if (g_debug) {
        fprintf(stderr, "nfqueue cb len=%d id=%u\n", len, id);
    }
    uint32_t verdict = goVerdict(id, payload, len);
    return nfq_set_verdict(qh, id, verdict, 0, NULL);
}

static int runQueue(int queueNum) {
    int g_rcvbuf = 0;
    char *debug_env = getenv("NFQUEUE_DEBUG");
    if (debug_env && debug_env[0] != '\0') {
        g_debug = 1;
    }
    char *rcvbuf_env = getenv("NFQUEUE_RCVBUF");
    if (rcvbuf_env && rcvbuf_env[0] != '\0') {
        g_rcvbuf = atoi(rcvbuf_env);
    }
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
    if (g_rcvbuf > 0) {
        setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &g_rcvbuf, sizeof(g_rcvbuf));
    }
    char buf[4096] __attribute__((aligned));
    while (1) {
        int rv = recv(fd, buf, sizeof(buf), 0);
        if (rv < 0) {
            if (errno == EINTR) continue;
            if (g_debug) {
                fprintf(stderr, "nfqueue recv error: %s\n", strerror(errno));
            }
            break;
        }
        if (g_debug) {
            fprintf(stderr, "nfqueue recv bytes=%d\n", rv);
        }
        if (g_debug) {
            fprintf(stderr, "nfq_handle_packet rv=%d\n", nfq_handle_packet(h, buf, rv));
        } else {
            nfq_handle_packet(h, buf, rv);
        }
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

const (
	udpProto           = 17
	defaultBlockedPort = 5555
)

//export goVerdict
func goVerdict(id C.uint32_t, data *C.uchar, length C.int) C.uint32_t {
	debug := os.Getenv("NFQUEUE_DEBUG") != ""
	if debug {
		fmt.Printf("goVerdict len=%d\n", int(length))
	}
	if data == nil || length <= 0 {
		return C.uint32_t(C.NF_ACCEPT)
	}

	b := C.GoBytes(unsafe.Pointer(data), length)
	if len(b) < 20 {
		if debug {
			fmt.Printf("nfqueue short packet len=%d\n", len(b))
		}
		return C.uint32_t(C.NF_ACCEPT)
	}

	ihl := int(b[0]&0x0f) * 4
	if ihl < 20 || len(b) < ihl+8 {
		if debug {
			fmt.Printf("nfqueue bad ihl=%d len=%d\n", ihl, len(b))
		}
		return C.uint32_t(C.NF_ACCEPT)
	}
	if debug {
		fmt.Printf("goVerdict ihl=%d proto=%d len=%d\n", ihl, b[9], len(b))
	}

	if b[9] != udpProto {
		if debug {
			fmt.Printf("nfqueue non-udp proto=%d ihl=%d len=%d\n", b[9], ihl, len(b))
		}
		return C.uint32_t(C.NF_ACCEPT)
	}

	srcPort := binary.BigEndian.Uint16(b[ihl : ihl+2])
	dstPort := binary.BigEndian.Uint16(b[ihl+2 : ihl+4])
	src := net.IP(b[12:16]).String()
	dst := net.IP(b[16:20]).String()
	if debug {
		fmt.Printf("goVerdict udp src=%d dst=%d\n", srcPort, dstPort)
	}

	blockedPort := blockedSrcPort()
	verdict := C.uint32_t(C.NF_ACCEPT)
	if srcPort == blockedPort {
		verdict = C.uint32_t(C.NF_DROP)
	}
	fmt.Printf("udp src_port=%d dst_port=%d %s -> %s verdict=%s\n",
		srcPort, dstPort, src, dst, verdictString(verdict))
	return verdict
}

func blockedSrcPort() uint16 {
	if val := os.Getenv("BLOCKED_SRC_PORT"); val != "" {
		if parsed, err := strconv.ParseUint(val, 10, 16); err == nil {
			return uint16(parsed)
		}
	}
	return defaultBlockedPort
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
	if os.Getenv("NFQUEUE_DEBUG") != "" || os.Getenv("NFQUEUE_RCVBUF") != "" {
		fmt.Printf("nfqueue_reader: env NFQUEUE_DEBUG=%q NFQUEUE_RCVBUF=%q\n",
			os.Getenv("NFQUEUE_DEBUG"), os.Getenv("NFQUEUE_RCVBUF"))
	}
	fmt.Printf("nfqueue_reader: queue %d blocked_src_port=%d\n", queueNum, blockedSrcPort())
	C.runQueue(C.int(queueNum))
}
