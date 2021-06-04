// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <algorithm>
#include "aio.h"

#include "common/debug.h"
#include "common/errno.h"
#include "include/assert.h"
#include "common/ceph_context.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_aio
#undef dout_prefix
#define dout_prefix *_dout << "aio "

#if defined(HAVE_LIBAIO)


int aio_queue_t::submit(aio_t &aio, int *retries)
{
  // 2^16 * 125us = ~8 seconds, so max sleep is ~16 seconds
  int attempts = 16;
  int delay = 125;
  iocb *piocb = &aio.iocb;
  int r;
  while (true) {
    r = io_submit(ctx, 1, &piocb);
    if (r < 0) {
      if (r == -EAGAIN && attempts-- > 0) {
	usleep(delay);
	delay *= 2;
	(*retries)++;
	continue;
      }
    }
    assert(r == 1);
    break;
  }
  return r;
}

std::ostream& operator<<(std::ostream& os, const aio_t& aio)
{
  unsigned i = 0;
  os << "aio: ";
  for (auto& iov : aio.iov) {
    os << "\n [" << i++ << "] 0x"
       << std::hex << iov.iov_base << "~" << iov.iov_len << std::dec;
  }
  return os;
}

int aio_queue_t::submit_batch(aio_iter begin, aio_iter end, 
			      uint64_t aios_size, void *priv,
			      int *retries, bool block)
{
  dout(3) << __func__ << " start." << dendl;
  // 2^16 * 125us = ~8 seconds, so max sleep is ~16 seconds
  int attempts = 16;
  int delay = 125;

  int done_all = 0;

  aio_iter cur = begin;

  int once_max_submit_aios = g_ceph_context->_conf->bdev_aio_once_submit_max;
  int submit_times = 0;
  if (aios_size % once_max_submit_aios == 0) {
    submit_times = aios_size/once_max_submit_aios;
  } else {
    submit_times = aios_size/once_max_submit_aios + 1;
  }
  dout(3) << __func__ << "submit times: " << submit_times << ", " << "aios size: "<< aios_size << dendl;
  // piocb maybe stack overflow, so submit ios many times instead of one time.
  for(int i=0; i < submit_times; i++) {
    struct iocb *piocb[once_max_submit_aios];
    int left = 0;
    while ((cur != end) && (left < once_max_submit_aios )) {
      cur->priv = priv;
      *(piocb+left) = &cur->iocb;
      ++left;
      ++cur;
    }
    dout(5) << __func__ << "now time: " << i << dendl;
    int done = 0;
    while (left > 0) {
      int r = io_submit(ctx, std::min(left, max_iodepth), piocb + done);
      if (r < 0) {
        if (r == -EAGAIN && attempts-- > 0) {
	  usleep(delay);
	  delay *= 2;
	  (*retries)++;
	  continue;
        }
        return r;
      }
      assert(r > 0);
      done += r;
      left -= r;
      attempts = 16;
      delay = 125;
    }
    done_all += done;
    if (block) {
      usleep(g_ceph_context->_conf->bdev_aio_submit_sleep*1000*1000);
      dout(3) << __func__ << " sleep."  << dendl;
    } else {
      dout(3) << __func__ << " no sleep" << dendl;
    }
  }
  dout(3) << __func__ << " end." << dendl;
  return done_all;
}

int aio_queue_t::get_next_completed(int timeout_ms, aio_t **paio, int max)
{
  io_event event[max];
  struct timespec t = {
    timeout_ms / 1000,
    (timeout_ms % 1000) * 1000 * 1000
  };

  int r = 0;
  do {
    r = io_getevents(ctx, 1, max, event, &t);
  } while (r == -EINTR);

  for (int i=0; i<r; ++i) {
    paio[i] = (aio_t *)event[i].obj;
    paio[i]->rval = event[i].res;
  }
  return r;
}

#endif
